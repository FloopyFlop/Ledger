from __future__ import annotations

import dataclasses
import json
import logging
import subprocess
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from .config import ProxySettings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HttpResponse:
    url: str
    final_url: str | None
    status_code: int | None
    headers: dict[str, str]
    body: bytes
    error: str | None
    content_type: str | None


class HttpClient:
    """Resilient HTTP client with proxy support and transport fallback order."""

    def __init__(
        self,
        *,
        proxy: ProxySettings,
        timeout_seconds: int,
        user_agent: str,
        expedition_path: str | None,
    ) -> None:
        self._proxy = proxy
        self._timeout_seconds = timeout_seconds
        self._user_agent = user_agent
        self._proxy_index = 0
        self._proxy_lock = threading.Lock()
        self._max_attempts = 2
        self._host_rate_lock = threading.Lock()
        self._host_next_allowed: dict[str, float] = {}
        self._metrics_lock = threading.Lock()
        self._proxy_attempt_count = 0
        self._direct_attempt_count = 0
        self._transport_counts: dict[str, int] = {}
        self._host_counts: dict[str, int] = {}
        self._error_count = 0

        self._expedition_fetcher: Any | None = None
        self._expedition_request_config_proxy: Any | None = None

        self._init_expedition(expedition_path)

        self._requests_session = requests.Session()
        # Never inherit proxy/no_proxy env vars; use only configured proxy values.
        self._requests_session.trust_env = False

    def fetch(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        prefer: str = "auto",
        _allow_http_fallback: bool = True,
    ) -> HttpResponse:
        merged_headers = {"User-Agent": self._user_agent}
        if headers:
            merged_headers.update(headers)

        host = (urllib.parse.urlsplit(url).hostname or "").lower()
        order = self._transport_order(host=host, prefer=prefer)

        # Proxy-only policy: never send direct requests.
        if not self._has_proxy():
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error="Proxy is required by policy for all requests",
                content_type=None,
            )
        proxy_modes = [True]

        last: HttpResponse | None = None
        for method in order:
            for use_proxy in proxy_modes:
                for attempt in range(1, self._max_attempts + 1):
                    self._respect_host_pacing(host)
                    response = self._fetch_once(
                        method=method,
                        url=url,
                        headers=merged_headers,
                        use_proxy=use_proxy,
                    )
                    self._record_attempt(
                        host=host,
                        method=method,
                        use_proxy=use_proxy,
                        response=response,
                    )
                    last = response
                    if not self._should_retry(response):
                        return response

                    if attempt < self._max_attempts:
                        wait_s = self._retry_wait_seconds(response, attempt)
                        logger.debug(
                            "Retrying %s request (%s/%s) for %s in %.1fs due to %s",
                            method,
                            attempt,
                            self._max_attempts,
                            url,
                            wait_s,
                            response.error or f"HTTP {response.status_code}",
                        )
                        time.sleep(wait_s)

                # If this method failed for non-proxy reasons, try next transport.
                break

        if (
            _allow_http_fallback
            and last is not None
            and last.error
            and self._tls_proxy_failure(last.error)
        ):
            fallback_url = self._http_fallback_url(url)
            if fallback_url and fallback_url != url:
                logger.debug("Retrying via HTTP fallback URL for %s -> %s", url, fallback_url)
                return self.fetch(
                    fallback_url,
                    headers=merged_headers,
                    prefer=prefer,
                    _allow_http_fallback=False,
                )

        return last or HttpResponse(
            url=url,
            final_url=None,
            status_code=None,
            headers={},
            body=b"",
            error="Unknown networking failure",
            content_type=None,
        )

    def fetch_text(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        prefer: str = "auto",
    ) -> tuple[str | None, str | None]:
        response = self.fetch(url, headers=headers, prefer=prefer)
        if response.error:
            return None, response.error
        text = response.body.decode("utf-8", errors="replace")
        if response.status_code and response.status_code >= 400:
            return None, f"HTTP {response.status_code}"
        return text, None

    def fetch_json(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        prefer: str = "auto",
    ) -> tuple[dict | list | None, str | None]:
        merged_headers = {"Accept": "application/json"}
        if headers:
            merged_headers.update(headers)
        text, error = self.fetch_text(url, headers=merged_headers, prefer=prefer)
        if error or not text:
            return None, error or "No response body"
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            return None, f"Invalid JSON: {exc}"
        return payload, None

    def proxy_stats(self) -> dict[str, Any]:
        with self._metrics_lock:
            return {
                "proxy_attempt_count": self._proxy_attempt_count,
                "direct_attempt_count": self._direct_attempt_count,
                "error_count": self._error_count,
                "transport_counts": dict(sorted(self._transport_counts.items())),
                "host_counts": dict(sorted(self._host_counts.items())),
            }

    def _transport_order(self, *, host: str, prefer: str) -> list[str]:
        if prefer in {"requests", "curl", "expedition"}:
            if (
                host.endswith("dblp.org")
                or host.endswith("openalex.org")
                or host.endswith("semanticscholar.org")
                or host.endswith("crossref.org")
                or host.endswith("arxiv.org")
            ):
                return [prefer]
            ordered = [prefer, "requests", "curl", "expedition"]
            return list(dict.fromkeys(ordered))

        # DBLP/OpenAlex are API-heavy and more stable via requests than Cloudscraper.
        if (
            host.endswith("dblp.org")
            or host.endswith("openalex.org")
            or host.endswith("semanticscholar.org")
            or host.endswith("crossref.org")
            or host.endswith("arxiv.org")
        ):
            return ["requests"]

        if self._expedition_fetcher is not None:
            return ["expedition", "requests", "curl"]
        return ["requests", "curl"]

    def _init_expedition(self, expedition_path: str | None) -> None:
        if expedition_path:
            repo = Path(expedition_path)
            candidate = repo / "expedition"
            if candidate.exists():
                sys.path.insert(0, str(candidate))
            elif repo.exists():
                sys.path.insert(0, str(repo))

        try:
            from expedition.config import ProxyConfig, RequestConfig
            from expedition.fetcher import CloudscraperFetcher, ProxySelector
        except Exception:
            logger.info("Expedition not importable; using requests/curl fallback stack")
            return

        proxy_config = ProxyConfig.from_dict(
            {
                "http": self._proxy.http,
                "https": self._proxy.https,
                "rotate": self._proxy.rotate,
                "pool": self._proxy.pool,
            }
        )
        base = {
            "timeout_seconds": self._timeout_seconds,
            "max_retries": 1,
            "retry_backoff_seconds": 0.5,
            "user_agent": self._user_agent,
            "headers": {},
        }

        self._expedition_request_config_proxy = RequestConfig(
            proxies=proxy_config,
            **base,
        )
        self._expedition_fetcher = CloudscraperFetcher(ProxySelector(proxy_config))
        logger.info("Using Expedition fetcher when suitable")

    def _fetch_once(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        use_proxy: bool,
    ) -> HttpResponse:
        if method == "requests":
            return self._fetch_via_requests(url, headers, use_proxy=use_proxy)
        if method == "curl":
            return self._fetch_via_curl(url, headers, use_proxy=use_proxy)
        if method == "expedition":
            return self._fetch_via_expedition(url, headers, use_proxy=use_proxy)

        return HttpResponse(
            url=url,
            final_url=None,
            status_code=None,
            headers={},
            body=b"",
            error=f"Unknown transport method: {method}",
            content_type=None,
        )

    def _fetch_via_expedition(
        self,
        url: str,
        headers: dict[str, str],
        *,
        use_proxy: bool,
    ) -> HttpResponse:
        if self._expedition_fetcher is None:
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error="Expedition fetcher unavailable",
                content_type=None,
            )

        if not use_proxy:
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error="Direct Expedition requests are disabled by proxy policy",
                content_type=None,
            )

        request_config = self._expedition_request_config_proxy
        if request_config is None:
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error="Expedition request config unavailable",
                content_type=None,
            )

        cfg = dataclasses.replace(request_config)
        cfg.headers = headers
        result = self._expedition_fetcher.fetch(url, cfg)
        return HttpResponse(
            url=result.url,
            final_url=result.final_url,
            status_code=result.status_code,
            headers=result.headers,
            body=result.body,
            error=result.error,
            content_type=result.content_type,
        )

    def _fetch_via_requests(
        self,
        url: str,
        headers: dict[str, str],
        *,
        use_proxy: bool,
    ) -> HttpResponse:
        proxies = self._next_proxy_dict(use_proxy=use_proxy)
        timeout_seconds = self._timeout_for_url(url)
        if use_proxy and not proxies:
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error="Proxy required but no proxy endpoint is configured",
                content_type=None,
            )
        try:
            response = self._requests_session.get(
                url,
                timeout=timeout_seconds,
                headers=headers,
                proxies=proxies,
                allow_redirects=True,
            )
            return HttpResponse(
                url=url,
                final_url=response.url,
                status_code=response.status_code,
                headers={str(k): str(v) for k, v in response.headers.items()},
                body=response.content or b"",
                error=None,
                content_type=response.headers.get("Content-Type"),
            )
        except Exception as exc:  # pragma: no cover - network-dependent
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error=str(exc),
                content_type=None,
            )

    def _fetch_via_curl(
        self,
        url: str,
        headers: dict[str, str],
        *,
        use_proxy: bool,
    ) -> HttpResponse:
        meta_tag = "__LEDGER_META__"
        timeout_seconds = self._timeout_for_url(url)
        cmd = [
            "curl",
            "-sS",
            "-L",
            "--max-time",
            str(timeout_seconds),
            "-w",
            f"\\n{meta_tag}%{{http_code}}|%{{url_effective}}|%{{content_type}}",
            url,
        ]

        for key, value in headers.items():
            cmd.extend(["-H", f"{key}: {value}"])

        if use_proxy:
            proxies = self._next_proxy_dict(use_proxy=True)
            proxy = (proxies or {}).get("https") or (proxies or {}).get("http")
            if proxy:
                cmd.extend(["--proxy", proxy, "--noproxy", ""])
            else:
                return HttpResponse(
                    url=url,
                    final_url=None,
                    status_code=None,
                    headers={},
                    body=b"",
                    error="Proxy required but no proxy endpoint is configured",
                    content_type=None,
                )

        try:
            proc = subprocess.run(cmd, check=False, capture_output=True)
        except Exception as exc:  # pragma: no cover - environment-dependent
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error=str(exc),
                content_type=None,
            )

        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace").strip()
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error=stderr or f"curl exited with {proc.returncode}",
                content_type=None,
            )

        marker = f"\n{meta_tag}".encode("utf-8")
        stdout = proc.stdout
        idx = stdout.rfind(marker)
        if idx < 0:
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=stdout,
                error=None,
                content_type=None,
            )

        body = stdout[:idx]
        meta = stdout[idx + len(marker) :].decode("utf-8", errors="replace").strip()
        code_str, _, rest = meta.partition("|")
        final_url, _, content_type = rest.partition("|")

        status_code: int | None
        try:
            status_code = int(code_str)
        except ValueError:
            status_code = None

        return HttpResponse(
            url=url,
            final_url=final_url or None,
            status_code=status_code,
            headers={},
            body=body,
            error=None,
            content_type=content_type or None,
        )

    @staticmethod
    def _should_retry(response: HttpResponse) -> bool:
        if response.error:
            return True
        if response.status_code in {408, 429, 500, 502, 503, 504}:
            return True
        return False

    def _has_proxy(self) -> bool:
        return bool(self._proxy.http or self._proxy.https or self._proxy.pool)

    @staticmethod
    def _retry_after_seconds(headers: dict[str, str]) -> float | None:
        for key, value in headers.items():
            if key.lower() == "retry-after":
                raw = str(value).strip()
                try:
                    return max(0.0, float(raw))
                except ValueError:
                    return None
        return None

    def _retry_wait_seconds(self, response: HttpResponse, attempt: int) -> float:
        if response.status_code == 429:
            retry_after = self._retry_after_seconds(response.headers)
            if retry_after is not None:
                return max(2.0, retry_after)
            return 8.0 * attempt
        if response.error and (
            "ProxyError" in response.error
            or "RemoteDisconnected" in response.error
            or "Connection reset by peer" in response.error
        ):
            return 2.0 * attempt
        return 0.75 * attempt

    @staticmethod
    def _min_interval_for_host(host: str) -> float:
        if host.endswith("dblp.org"):
            return 2.0
        if host.endswith("semanticscholar.org"):
            return 0.35
        if host.endswith("crossref.org"):
            return 0.25
        if host.endswith("arxiv.org"):
            return 0.35
        if host.endswith("scholar.google.com"):
            return 2.0
        return 0.0

    def _respect_host_pacing(self, host: str) -> None:
        min_interval = self._min_interval_for_host(host)
        if min_interval <= 0:
            return

        wait_for = 0.0
        with self._host_rate_lock:
            now = time.monotonic()
            next_allowed = self._host_next_allowed.get(host, now)
            if next_allowed > now:
                wait_for = next_allowed - now
                scheduled = next_allowed + min_interval
            else:
                scheduled = now + min_interval
            self._host_next_allowed[host] = scheduled

        if wait_for > 0:
            time.sleep(wait_for)

    def _timeout_for_url(self, url: str) -> int:
        host = (urllib.parse.urlsplit(url).hostname or "").lower()
        if host.endswith("dblp.org"):
            return min(self._timeout_seconds, 12)
        return self._timeout_seconds

    def _next_proxy_dict(self, *, use_proxy: bool) -> dict[str, str] | None:
        if not use_proxy:
            return None

        if self._proxy.pool:
            with self._proxy_lock:
                if self._proxy.rotate:
                    chosen = self._proxy.pool[self._proxy_index % len(self._proxy.pool)]
                    self._proxy_index += 1
                else:
                    chosen = self._proxy.pool[0]
            return {"http": chosen, "https": chosen}

        chosen_http = self._proxy.http or self._proxy.https
        chosen_https = self._proxy.https or self._proxy.http
        proxies: dict[str, str] = {}
        if chosen_http:
            proxies["http"] = chosen_http
        if chosen_https:
            proxies["https"] = chosen_https
        return proxies or None

    @staticmethod
    def _tls_proxy_failure(error_text: str) -> bool:
        text = (error_text or "").lower()
        return (
            "ssleoferror" in text
            or "ssl_error_syscall" in text
            or "ssl:" in text
            or "ssl_connect" in text
        )

    @staticmethod
    def _http_fallback_url(url: str) -> str | None:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme.lower() != "https":
            return None

        host = (parsed.hostname or "").lower()
        allow_hosts = {
            "api.openalex.org",
            "openalex.org",
            "api.semanticscholar.org",
            "api.crossref.org",
            "dblp.org",
            "export.arxiv.org",
            "scholar.google.com",
            "aimi.cornell.edu",
        }
        if host not in allow_hosts:
            return None

        fallback = urllib.parse.urlunsplit(("http", parsed.netloc, parsed.path, parsed.query, parsed.fragment))
        return fallback

    def _record_attempt(
        self,
        *,
        host: str,
        method: str,
        use_proxy: bool,
        response: HttpResponse,
    ) -> None:
        host_key = host or "<unknown>"
        with self._metrics_lock:
            if use_proxy:
                self._proxy_attempt_count += 1
            else:
                self._direct_attempt_count += 1
            self._transport_counts[method] = self._transport_counts.get(method, 0) + 1
            self._host_counts[host_key] = self._host_counts.get(host_key, 0) + 1
            if response.error:
                self._error_count += 1
