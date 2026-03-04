from __future__ import annotations

import json
import os
import urllib.parse
from urllib.parse import quote
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_AWARD_PATTERNS = [
    "DMR-2433348",
    "DMR2433348",
    "DMR 2433348",
    "Award No. DMR-2433348",
]

DEFAULT_AIMI_MEMBER_NAMES = [
    "Adam Braunschweig",
    "Ala Santos",
    "Anil Damle",
    "B. Andrei Bernevig",
    "Carla Gomes",
    "Darrell Schlom",
    "Emilia Morosan",
    "Eun-Ah Kim",
    "Fengqi You",
    "Jennifer Sun",
    "John Thickstun",
    "Keith Brown",
    "Kilian Weinberger",
    "Kin Fai Mak",
    "Leslie Schoop",
    "Nicholas Abbott",
    "Peter Frazier",
    "Tess Smidt",
    "Yoav Artzi",
    "Zhigang Zhu",
]


def _load_env_file(env_file: Path) -> None:
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_optional_int(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    out = value.strip()
    return out or None


def _normalize_proxy_url(value: str | None) -> str | None:
    raw = _clean_optional(value)
    if not raw:
        return None
    parsed = urllib.parse.urlsplit(raw)
    if not parsed.scheme:
        raw = f"http://{raw}"
        parsed = urllib.parse.urlsplit(raw)

    # Some Webshare rotating proxies work on :9999 while :80 breaks TLS handshakes.
    # Auto-promote only for this known host unless explicitly disabled.
    host = (parsed.hostname or "").lower()
    port = parsed.port
    keep_webshare_port = _get_bool("LEDGER_WEBSHARE_KEEP_ORIGINAL_PORT", False)
    if host == "p.webshare.io" and port == 80 and not keep_webshare_port:
        username = parsed.username or ""
        password = parsed.password or ""
        auth = ""
        if username:
            auth = quote(username, safe="")
            if password:
                auth += ":" + quote(password, safe="")
            auth += "@"
        netloc = f"{auth}{host}:9999"
        parsed = parsed._replace(netloc=netloc)

    # Strip trailing slash-only path because some proxy clients treat it as part of host.
    path = parsed.path
    if path == "/":
        path = ""
    normalized = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))
    return normalized


def _normalize_name_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalpha())


def _parse_list_env(name: str, default: list[str]) -> list[str]:
    raw = _clean_optional(os.getenv(name))
    if not raw:
        return list(default)
    values = [part.strip() for part in raw.split(",") if part.strip()]
    return values or list(default)


@dataclass(slots=True)
class ProxySettings:
    http: str | None = None
    https: str | None = None
    rotate: bool = False
    pool: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SourceSettings:
    dblp: bool = True
    openalex: bool = True
    semantic_scholar: bool = True
    crossref: bool = True
    arxiv: bool = True
    google_scholar: bool = True

    def enabled_names(self) -> list[str]:
        out: list[str] = []
        if self.dblp:
            out.append("dblp")
        if self.openalex:
            out.append("openalex")
        if self.semantic_scholar:
            out.append("semantic_scholar")
        if self.crossref:
            out.append("crossref")
        if self.arxiv:
            out.append("arxiv")
        if self.google_scholar:
            out.append("google_scholar")
        return out


@dataclass(slots=True)
class LedgerConfig:
    team_url: str = "https://aimi.cornell.edu/team/"
    output_dir: Path = Path("output")
    request_timeout_seconds: int = 25
    user_agent: str = "Ledger/2.0 (+AIMI publication aggregation)"
    expedition_path: str | None = "/Users/abm/XVOL/ABM/Projects/Code/Expedition"

    lookback_years: int = 2
    member_limit: int | None = None
    workers: int = 2
    max_results_per_member_per_source: int = 300
    max_google_scholar_pages: int = 2
    include_raw_payloads: bool = False
    probe_sources_before_collection: bool = True
    fallback_member_names: list[str] = field(default_factory=lambda: list(DEFAULT_AIMI_MEMBER_NAMES))

    award_patterns: list[str] = field(default_factory=lambda: list(DEFAULT_AWARD_PATTERNS))

    semantic_scholar_api_key: str | None = None
    crossref_mailto: str | None = None
    serpapi_api_key: str | None = None

    dblp_author_search_api: str = "https://dblp.org/search/author/api"
    dblp_pid_xml_template: str = "https://dblp.org/pid/{pid}.xml"
    dblp_pid_overrides: dict[str, str] = field(default_factory=dict)

    openalex_author_search_api: str = "https://api.openalex.org/authors"
    openalex_works_api: str = "https://api.openalex.org/works"

    semantic_scholar_author_search_api: str = "https://api.semanticscholar.org/graph/v1/author/search"
    semantic_scholar_author_papers_api_template: str = "https://api.semanticscholar.org/graph/v1/author/{author_id}/papers"

    crossref_works_api: str = "https://api.crossref.org/works"
    arxiv_api: str = "http://export.arxiv.org/api/query"
    google_scholar_search_url: str = "https://scholar.google.com/scholar"
    google_scholar_serpapi_api: str = "https://serpapi.com/search.json"

    sources: SourceSettings = field(default_factory=SourceSettings)
    proxy: ProxySettings = field(default_factory=ProxySettings)

    @classmethod
    def from_env(cls, env_file: Path = Path(".env")) -> "LedgerConfig":
        defaults = cls()
        env_file = env_file.expanduser().resolve()
        _load_env_file(env_file)

        output_dir = Path(os.getenv("LEDGER_OUTPUT_DIR", "output")).expanduser()
        if not output_dir.is_absolute():
            output_dir = (env_file.parent / output_dir).resolve()

        proxy_url = _normalize_proxy_url(os.getenv("LEDGER_PROXY_URL"))
        proxy_http = _normalize_proxy_url(os.getenv("LEDGER_PROXY_HTTP"))
        proxy_https = _normalize_proxy_url(os.getenv("LEDGER_PROXY_HTTPS"))
        pool_raw = _clean_optional(os.getenv("LEDGER_PROXY_POOL"))
        pool = [_normalize_proxy_url(x.strip()) for x in (pool_raw or "").split(",") if x.strip()]
        pool = [value for value in pool if value]

        pid_overrides_raw = _clean_optional(os.getenv("LEDGER_DBLP_PID_OVERRIDES_JSON"))
        pid_overrides: dict[str, str] = {}
        if pid_overrides_raw:
            try:
                parsed = json.loads(pid_overrides_raw)
                if isinstance(parsed, dict):
                    for key, value in parsed.items():
                        k = _normalize_name_key(str(key))
                        v = str(value).strip()
                        if k and v:
                            pid_overrides[k] = v
            except json.JSONDecodeError:
                pass

        proxy = ProxySettings(
            http=proxy_http or proxy_url,
            https=proxy_https or proxy_url,
            rotate=_get_bool("LEDGER_PROXY_ROTATE", False),
            pool=pool,
        )

        sources = SourceSettings(
            dblp=_get_bool("LEDGER_ENABLE_DBLP", True),
            openalex=_get_bool("LEDGER_ENABLE_OPENALEX", True),
            semantic_scholar=_get_bool("LEDGER_ENABLE_SEMANTIC_SCHOLAR", True),
            crossref=_get_bool("LEDGER_ENABLE_CROSSREF", True),
            arxiv=_get_bool("LEDGER_ENABLE_ARXIV", True),
            google_scholar=_get_bool("LEDGER_ENABLE_GOOGLE_SCHOLAR", True),
        )

        config = cls(
            team_url=os.getenv("LEDGER_TEAM_URL", defaults.team_url),
            output_dir=output_dir,
            request_timeout_seconds=max(1, _get_int("LEDGER_REQUEST_TIMEOUT_SECONDS", 25)),
            user_agent=os.getenv("LEDGER_USER_AGENT", defaults.user_agent),
            expedition_path=_clean_optional(os.getenv("LEDGER_EXPEDITION_PATH")) or defaults.expedition_path,
            lookback_years=max(0, _get_int("LEDGER_LOOKBACK_YEARS", 2)),
            member_limit=_get_optional_int("LEDGER_MEMBER_LIMIT"),
            workers=max(1, _get_int("LEDGER_WORKERS", 2)),
            max_results_per_member_per_source=max(10, _get_int("LEDGER_MAX_RESULTS_PER_MEMBER_PER_SOURCE", 300)),
            max_google_scholar_pages=max(1, _get_int("LEDGER_MAX_GOOGLE_SCHOLAR_PAGES", 2)),
            include_raw_payloads=_get_bool("LEDGER_INCLUDE_RAW_PAYLOADS", False),
            probe_sources_before_collection=_get_bool("LEDGER_PROBE_SOURCES_BEFORE_COLLECTION", True),
            fallback_member_names=_parse_list_env("LEDGER_MEMBER_NAMES", DEFAULT_AIMI_MEMBER_NAMES),
            award_patterns=_parse_list_env("LEDGER_AWARD_PATTERNS", DEFAULT_AWARD_PATTERNS),
            semantic_scholar_api_key=_clean_optional(os.getenv("LEDGER_SEMANTIC_SCHOLAR_API_KEY")),
            crossref_mailto=_clean_optional(os.getenv("LEDGER_CROSSREF_MAILTO")),
            serpapi_api_key=_clean_optional(os.getenv("LEDGER_SERPAPI_API_KEY")),
            dblp_author_search_api=os.getenv("LEDGER_DBLP_AUTHOR_SEARCH_API", defaults.dblp_author_search_api),
            dblp_pid_xml_template=os.getenv("LEDGER_DBLP_PID_XML_TEMPLATE", defaults.dblp_pid_xml_template),
            dblp_pid_overrides=pid_overrides,
            openalex_author_search_api=os.getenv("LEDGER_OPENALEX_AUTHOR_SEARCH_API", defaults.openalex_author_search_api),
            openalex_works_api=os.getenv("LEDGER_OPENALEX_WORKS_API", defaults.openalex_works_api),
            semantic_scholar_author_search_api=os.getenv(
                "LEDGER_SEMANTIC_SCHOLAR_AUTHOR_SEARCH_API",
                defaults.semantic_scholar_author_search_api,
            ),
            semantic_scholar_author_papers_api_template=os.getenv(
                "LEDGER_SEMANTIC_SCHOLAR_AUTHOR_PAPERS_API_TEMPLATE",
                defaults.semantic_scholar_author_papers_api_template,
            ),
            crossref_works_api=os.getenv("LEDGER_CROSSREF_WORKS_API", defaults.crossref_works_api),
            arxiv_api=os.getenv("LEDGER_ARXIV_API", defaults.arxiv_api),
            google_scholar_search_url=os.getenv("LEDGER_GOOGLE_SCHOLAR_SEARCH_URL", defaults.google_scholar_search_url),
            google_scholar_serpapi_api=os.getenv("LEDGER_GOOGLE_SCHOLAR_SERPAPI_API", defaults.google_scholar_serpapi_api),
            sources=sources,
            proxy=proxy,
        )
        return config
