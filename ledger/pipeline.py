from __future__ import annotations

import json
import logging
import re
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from .collectors import (
    collect_arxiv_for_member,
    collect_crossref_for_member,
    collect_dblp_for_member,
    collect_google_scholar_for_member,
    collect_openalex_for_member,
    collect_semantic_scholar_for_member,
)
from .config import LedgerConfig
from .funding import compile_award_regexes, find_award_context, find_award_mentions
from .models import CanonicalPaper, CollectionRunSummary, Member, SourcePaperRecord, to_json_dict
from .net import HttpClient
from .pdfs import download_pdf, extract_text_from_pdf, safe_file_stem
from .team import parse_team_members

logger = logging.getLogger(__name__)

SOURCE_ORDER = [
    "dblp",
    "openalex",
    "semantic_scholar",
    "crossref",
    "arxiv",
    "google_scholar",
]


class _LiveStatus:
    """Single-line renderer for long collection stages."""

    def __init__(self) -> None:
        self._enabled = sys.stderr.isatty()
        self._last_line_len = 0
        self._last_emit_at = 0.0

    def update(self, message: str, *, force: bool = False, min_interval: float = 0.15) -> None:
        if not self._enabled:
            return
        now = time.monotonic()
        if not force and (now - self._last_emit_at) < min_interval:
            return

        text = " ".join(message.split())
        padded = text
        if len(text) < self._last_line_len:
            padded = text + (" " * (self._last_line_len - len(text)))

        self._last_emit_at = now
        self._last_line_len = max(self._last_line_len, len(text))
        sys.stderr.write("\r" + padded[:220])
        sys.stderr.flush()

    def clear(self) -> None:
        if not self._enabled or self._last_line_len == 0:
            return
        sys.stderr.write("\r" + (" " * self._last_line_len) + "\r")
        sys.stderr.flush()
        self._last_line_len = 0


def run_ledger(
    config: LedgerConfig,
    *,
    member_limit_override: int | None = None,
    lookback_years_override: int | None = None,
) -> CollectionRunSummary:
    run_clock_start = time.monotonic()
    if not (config.proxy.http or config.proxy.https or config.proxy.pool):
        raise RuntimeError("Proxy is required by policy. Set LEDGER_PROXY_URL/HTTP/HTTPS in .env.")

    run_started_at = _utc_now()
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%SZ")

    base_output = config.output_dir
    run_dir = base_output / "runs" / timestamp
    latest_dir = base_output / "latest"
    run_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    status = _LiveStatus()
    client = HttpClient(
        proxy=config.proxy,
        timeout_seconds=config.request_timeout_seconds,
        user_agent=config.user_agent,
        expedition_path=config.expedition_path,
    )

    logger.info("Fetching team page: %s", config.team_url)
    team_html, team_error = client.fetch_text(config.team_url)
    if team_error or not team_html:
        if config.fallback_member_names:
            logger.warning(
                "Failed to fetch team page (%s). Using configured fallback member list (%d names).",
                team_error,
                len(config.fallback_member_names),
            )
            members = [Member(name=name, source_url=config.team_url) for name in config.fallback_member_names]
        else:
            raise RuntimeError(f"Failed to fetch team page: {team_error}")
    else:
        members = parse_team_members(team_html, config.team_url)
        if not members and config.fallback_member_names:
            logger.warning(
                "Team page parsed with zero members. Using configured fallback member list (%d names).",
                len(config.fallback_member_names),
            )
            members = [Member(name=name, source_url=config.team_url) for name in config.fallback_member_names]
    member_limit = member_limit_override if member_limit_override is not None else config.member_limit
    if member_limit is not None:
        members = members[: max(0, member_limit)]

    lookback_years = lookback_years_override if lookback_years_override is not None else config.lookback_years
    min_year = datetime.now(tz=timezone.utc).year - max(0, lookback_years)

    _write_json(run_dir / "members.json", [to_json_dict(member) for member in members])

    source_record_counts: dict[str, int] = {}
    source_error_counts: dict[str, int] = {}
    source_errors: list[dict[str, str]] = []
    source_records_by_name: dict[str, list[SourcePaperRecord]] = {}
    source_probe_status: dict[str, str] = {}

    enabled_sources = [source for source in SOURCE_ORDER if source in config.sources.enabled_names()]
    logger.info("Enabled sources: %s", ", ".join(enabled_sources) if enabled_sources else "none")

    active_sources = list(enabled_sources)
    if config.probe_sources_before_collection and enabled_sources:
        probe_started = time.monotonic()
        probed_active: list[str] = []
        for source_name in enabled_sources:
            ok, message = _probe_source_connectivity(
                source_name=source_name,
                client=client,
                config=config,
                min_year=min_year,
            )
            if ok:
                source_probe_status[source_name] = "ok"
                probed_active.append(source_name)
            else:
                source_probe_status[source_name] = f"skipped: {message}"
                source_record_counts[source_name] = 0
                source_error_counts[source_name] = 1
                source_records_by_name[source_name] = []
                source_errors.append(
                    {
                        "source": source_name,
                        "member": "<probe>",
                        "error": message,
                    }
                )
                _write_json(run_dir / "sources" / f"{source_name}.json", [])
                logger.warning("Skipping source %s after failed proxy probe: %s", source_name, message)
        active_sources = probed_active
        logger.info(
            "Source probe stage completed in %s (%d active / %d enabled)",
            _format_elapsed(time.monotonic() - probe_started),
            len(active_sources),
            len(enabled_sources),
        )
    else:
        for source_name in enabled_sources:
            source_probe_status[source_name] = "not_checked"

    for source_name in active_sources:
        source_started = time.monotonic()
        logger.info("Collecting %s records for %d member(s)", source_name, len(members))
        records, errors = _collect_source_for_members(
            source_name=source_name,
            members=members,
            client=client,
            config=config,
            min_year=min_year,
            status=status,
        )
        source_records_by_name[source_name] = records
        source_record_counts[source_name] = len(records)
        source_error_counts[source_name] = len(errors)

        if errors:
            for item in errors:
                source_errors.append({"source": source_name, **item})

        _write_json(run_dir / "sources" / f"{source_name}.json", [to_json_dict(record) for record in records])
        logger.info(
            "Finished %s: %d record(s), %d error(s) in %s",
            source_name,
            len(records),
            len(errors),
            _format_elapsed(time.monotonic() - source_started),
        )

    status.clear()

    all_records = [record for records in source_records_by_name.values() for record in records]
    award_regexes = compile_award_regexes(config.award_patterns)
    canonicalize_started = time.monotonic()
    canonical_papers = _canonicalize_records(all_records, award_regexes=award_regexes)
    logger.info(
        "Canonicalized %d raw records into %d papers in %s",
        len(all_records),
        len(canonical_papers),
        _format_elapsed(time.monotonic() - canonicalize_started),
    )

    enrich_started = time.monotonic()
    enrich_stats = _enrich_pdf_candidates(canonical_papers, client=client, config=config, status=status)
    status.clear()
    logger.info(
        (
            "PDF candidate enrichment completed in %s "
            "(papers=%d, with_pdf=%d, newly_filled=%d, doi_lookups=%d)"
        ),
        _format_elapsed(time.monotonic() - enrich_started),
        enrich_stats["papers_total"],
        enrich_stats["papers_with_pdf_after_enrich"],
        enrich_stats["filled_count"],
        enrich_stats["doi_lookup_count"],
    )

    if config.scan_pdfs_for_awards:
        scan_started = time.monotonic()
        scan_stats = _scan_awards_from_documents(
            papers=canonical_papers,
            client=client,
            run_dir=run_dir,
            award_regexes=award_regexes,
            max_pdf_mb=config.pdf_scan_max_mb,
            max_pages=config.pdf_scan_max_pages,
            max_candidates_per_paper=config.pdf_scan_max_candidates_per_paper,
            status=status,
        )
        status.clear()
        logger.info(
            (
                "Document award scan completed in %s "
                "(papers=%d, mentions=%d, no_pdf=%d, download_fail=%d, extract_fail=%d)"
            ),
            _format_elapsed(time.monotonic() - scan_started),
            scan_stats["papers_total"],
            scan_stats["mentions_count"],
            scan_stats["no_pdf_count"],
            scan_stats["download_fail_count"],
            scan_stats["extract_fail_count"],
        )
    canonical_papers.sort(key=lambda paper: ((paper.year or 0), paper.title.lower()), reverse=True)

    papers_with_award = [paper for paper in canonical_papers if paper.award_mentioned_in_metadata]
    papers_without_award = [paper for paper in canonical_papers if not paper.award_mentioned_in_metadata]

    proxy_stats = client.proxy_stats()

    run_finished_at = _utc_now()
    summary = CollectionRunSummary(
        run_started_at=run_started_at,
        run_finished_at=run_finished_at,
        output_dir=str(run_dir),
        team_member_count=len(members),
        lookback_years=lookback_years,
        source_record_counts=source_record_counts,
        source_error_counts=source_error_counts,
        source_probe_status=source_probe_status,
        raw_record_count=len(all_records),
        canonical_paper_count=len(canonical_papers),
        award_match_count=len(papers_with_award),
        proxy_attempt_count=int(proxy_stats.get("proxy_attempt_count", 0)),
        direct_attempt_count=int(proxy_stats.get("direct_attempt_count", 0)),
    )

    if summary.direct_attempt_count > 0:
        raise RuntimeError(
            f"Proxy policy violation detected: {summary.direct_attempt_count} direct request attempt(s) were made."
        )

    _write_json(run_dir / "summary.json", to_json_dict(summary))
    _write_json(run_dir / "source_errors.json", source_errors)
    _write_json(run_dir / "proxy_audit.json", proxy_stats)
    _write_json(run_dir / "papers_canonical.json", [to_json_dict(paper) for paper in canonical_papers])
    _write_json(
        run_dir / "papers_with_award_mention.json",
        [to_json_dict(paper) for paper in papers_with_award],
    )
    _write_json(
        run_dir / "papers_without_award_mention.json",
        [to_json_dict(paper) for paper in papers_without_award],
    )

    report = _render_report(summary, papers_with_award)
    (run_dir / "report.md").write_text(report, encoding="utf-8")

    # Convenience latest snapshots.
    _write_json(latest_dir / "summary.json", to_json_dict(summary))
    _write_json(latest_dir / "papers_canonical.json", [to_json_dict(paper) for paper in canonical_papers])
    _write_json(
        latest_dir / "papers_with_award_mention.json",
        [to_json_dict(paper) for paper in papers_with_award],
    )

    logger.info("Ledger pipeline finished in %s", _format_elapsed(time.monotonic() - run_clock_start))

    return summary


def _collect_source_for_members(
    *,
    source_name: str,
    members: list[Member],
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
    status: _LiveStatus,
) -> tuple[list[SourcePaperRecord], list[dict[str, str]]]:
    collector = _collector_for(source_name)
    workers = _workers_for_source(source_name, config.workers)

    records: list[SourcePaperRecord] = []
    errors: list[dict[str, str]] = []

    if source_name == "dblp":
        # DBLP performs better with single-threaded access and shared search cache.
        search_cache: dict[str, list[dict]] = {}
        total = len(members)
        status.update(f"[{source_name}] 0/{total} members | records 0 | errors 0", force=True)
        for index, member in enumerate(members, start=1):
            member_records, error = collect_dblp_for_member(
                member,
                client,
                config,
                min_year,
                search_cache=search_cache,
            )
            records.extend(member_records)
            if error:
                errors.append({"member": member.name, "error": _normalize_error(error)})
            status.update(
                f"[{source_name}] {index}/{total} members | records {len(records)} | errors {len(errors)}"
            )
        status.clear()
        return records, errors

    total = len(members)
    status.update(f"[{source_name}] 0/{total} members | records 0 | errors 0", force=True)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(collector, member, client, config, min_year): member
            for member in members
        }
        completed = 0
        for future in as_completed(futures):
            completed += 1
            member = futures[future]
            try:
                member_records, error = future.result()
            except Exception as exc:  # pragma: no cover - safety net for network parsing paths.
                member_records, error = [], str(exc)

            records.extend(member_records)
            if error:
                errors.append({"member": member.name, "error": _normalize_error(error)})

            status.update(
                f"[{source_name}] {completed}/{total} members | records {len(records)} | errors {len(errors)}"
            )

    status.clear()
    return records, errors


def _collector_for(source_name: str):
    collectors = {
        "dblp": collect_dblp_for_member,
        "openalex": collect_openalex_for_member,
        "semantic_scholar": collect_semantic_scholar_for_member,
        "crossref": collect_crossref_for_member,
        "arxiv": collect_arxiv_for_member,
        "google_scholar": collect_google_scholar_for_member,
    }
    if source_name not in collectors:
        raise KeyError(f"Unknown source collector: {source_name}")
    return collectors[source_name]


def _workers_for_source(source_name: str, default_workers: int) -> int:
    if source_name in {"dblp", "google_scholar"}:
        return 1
    if source_name == "arxiv":
        return max(1, min(default_workers, 2))
    return max(1, default_workers)


def _canonicalize_records(
    records: list[SourcePaperRecord],
    *,
    award_regexes,
) -> list[CanonicalPaper]:
    merged: dict[str, CanonicalPaper] = {}

    for record in records:
        title = _clean_text(record.title)
        if not title:
            continue

        canonical_id = _canonical_id(record)
        source_meta = {
            "source": record.source,
            "source_id": record.source_id,
            "member_name": record.member_name,
            "relevance_score": record.relevance_score,
            "landing_page_url": record.landing_page_url,
            "pdf_url": record.pdf_url,
        }

        if canonical_id not in merged:
            combined_text = " ".join(part for part in [title, record.abstract or ""] if part)
            mentions = find_award_mentions(combined_text, award_regexes)
            merged[canonical_id] = CanonicalPaper(
                canonical_id=canonical_id,
                title=title,
                normalized_title=_normalize_title(title),
                year=record.year,
                published_date=record.published_date,
                venue=record.venue,
                doi=record.doi,
                authors=list(record.authors),
                aimi_members=[record.member_name],
                abstract=record.abstract,
                urls=[record.landing_page_url] if record.landing_page_url else [],
                pdf_urls=[record.pdf_url] if record.pdf_url else [],
                sources=[record.source],
                source_records=[source_meta],
                award_mentioned_in_metadata=bool(mentions),
                award_mentions=mentions,
            )
            continue

        existing = merged[canonical_id]
        existing.title = _prefer_better_title(existing.title, title)
        existing.normalized_title = _normalize_title(existing.title)
        existing.year = _pick_year(existing.year, record.year)
        existing.published_date = _pick_published_date(existing.published_date, record.published_date)
        existing.venue = existing.venue or record.venue
        existing.doi = existing.doi or record.doi

        existing.authors = _merge_string_lists(existing.authors, record.authors)
        existing.aimi_members = _merge_string_lists(existing.aimi_members, [record.member_name])

        if record.abstract and (existing.abstract is None or len(record.abstract) > len(existing.abstract)):
            existing.abstract = record.abstract

        existing.urls = _merge_string_lists(existing.urls, [record.landing_page_url] if record.landing_page_url else [])
        existing.pdf_urls = _merge_string_lists(existing.pdf_urls, [record.pdf_url] if record.pdf_url else [])
        existing.sources = _merge_string_lists(existing.sources, [record.source])
        existing.source_records.append(source_meta)

        combined_text = " ".join(part for part in [existing.title, existing.abstract or "", record.abstract or ""] if part)
        mentions = find_award_mentions(combined_text, award_regexes)
        existing.award_mentions = _merge_string_lists(existing.award_mentions, mentions)
        existing.award_mentioned_in_metadata = bool(existing.award_mentions)

    return list(merged.values())


def _enrich_pdf_candidates(
    papers: list[CanonicalPaper],
    *,
    client: HttpClient,
    config: LedgerConfig,
    status: _LiveStatus | None = None,
) -> dict[str, int]:
    total = len(papers)
    stage_started = time.monotonic()
    doi_cache: dict[str, list[str]] = {}
    lookup_count = 0
    filled_count = 0

    for idx, paper in enumerate(papers, start=1):
        had_pdf = bool(paper.pdf_urls)
        paper.pdf_urls = _derive_pdf_candidates_for_canonical(paper)
        doi = (paper.doi or "").lower()
        if paper.pdf_urls or not doi:
            if not had_pdf and paper.pdf_urls:
                filled_count += 1
        else:
            if doi not in doi_cache:
                lookup_count += 1
                doi_cache[doi] = _lookup_openalex_pdf_candidates_for_doi(client=client, config=config, doi=doi)
            if doi_cache[doi]:
                paper.pdf_urls = _merge_string_lists(paper.pdf_urls, doi_cache[doi])
                filled_count += 1

        if status:
            status.update(
                (
                    f"[paper-enrich] {_progress_bar(idx, total)} {idx}/{total} "
                    f"| filled {filled_count} | doi_lookups {lookup_count} "
                    f"| elapsed {_format_elapsed(time.monotonic() - stage_started)}"
                ),
                force=(idx == total),
                min_interval=0.1,
            )
    return {
        "papers_total": total,
        "papers_with_pdf_after_enrich": sum(1 for paper in papers if bool(paper.pdf_urls)),
        "filled_count": filled_count,
        "doi_lookup_count": lookup_count,
    }


def _derive_pdf_candidates_for_canonical(paper: CanonicalPaper) -> list[str]:
    candidates: list[str] = []
    candidates = _merge_string_lists(candidates, paper.pdf_urls)

    hints: list[str] = []
    if paper.doi:
        hints.append(paper.doi)
    hints.extend(paper.urls)

    for source_meta in paper.source_records:
        for key in ("pdf_url", "landing_page_url", "source_id"):
            raw = source_meta.get(key)
            if isinstance(raw, str) and raw.strip():
                hints.append(raw.strip())

    for hint in hints:
        lowered = hint.lower()
        if lowered.endswith(".pdf"):
            candidates = _merge_string_lists(candidates, [hint])

        arxiv_id = _extract_arxiv_id(hint)
        if arxiv_id:
            candidates = _merge_string_lists(candidates, [f"https://arxiv.org/pdf/{arxiv_id}.pdf"])

    return candidates


def _lookup_openalex_pdf_candidates_for_doi(
    *,
    client: HttpClient,
    config: LedgerConfig,
    doi: str,
) -> list[str]:
    clean_doi = _clean_text(doi).strip()
    if not clean_doi:
        return []

    filter_expr = f"doi:https://doi.org/{clean_doi}"
    encoded = urllib.parse.quote(filter_expr)
    url = f"{config.openalex_works_api}?filter={encoded}&per-page=1"
    payload, error = client.fetch_json(url, headers={"Accept": "application/json"}, prefer="requests")
    if error or not isinstance(payload, dict):
        return []

    rows = payload.get("results")
    if not isinstance(rows, list) or not rows:
        return []

    work = rows[0]
    if not isinstance(work, dict):
        return []
    return _openalex_work_pdf_candidates(work)


def _openalex_work_pdf_candidates(work: dict) -> list[str]:
    candidates: list[str] = []
    best_oa = work.get("best_oa_location") if isinstance(work.get("best_oa_location"), dict) else {}
    primary = work.get("primary_location") if isinstance(work.get("primary_location"), dict) else {}

    for candidate in [
        best_oa.get("pdf_url"),
        best_oa.get("landing_page_url"),
        primary.get("pdf_url"),
        primary.get("landing_page_url"),
    ]:
        if not isinstance(candidate, str):
            continue
        value = _clean_text(candidate).strip()
        if not value:
            continue
        if value.lower().endswith(".pdf") or "/pdf" in value.lower():
            candidates = _merge_string_lists(candidates, [value])
        arxiv_id = _extract_arxiv_id(value)
        if arxiv_id:
            candidates = _merge_string_lists(candidates, [f"https://arxiv.org/pdf/{arxiv_id}.pdf"])

    return candidates


def _scan_awards_from_documents(
    *,
    papers: list[CanonicalPaper],
    client: HttpClient,
    run_dir: Path,
    award_regexes,
    max_pdf_mb: int,
    max_pages: int,
    max_candidates_per_paper: int,
    status: _LiveStatus | None = None,
) -> dict[str, int]:
    temp_pdf_dir = run_dir / "_pdf_cache"
    temp_pdf_dir.mkdir(parents=True, exist_ok=True)

    total = len(papers)
    stage_started = time.monotonic()
    mention_count = 0
    no_pdf_count = 0
    download_fail_count = 0
    extract_fail_count = 0

    for idx, paper in enumerate(papers, start=1):
        if not paper.pdf_urls:
            paper.document_scan_error = "No PDF candidates"
            no_pdf_count += 1
            if status:
                status.update(
                    (
                        f"[paper-scan] {_progress_bar(idx, total)} {idx}/{total} "
                        f"| mentions {mention_count} | no_pdf {no_pdf_count} "
                        f"| dl_fail {download_fail_count} | extract_fail {extract_fail_count} "
                        f"| elapsed {_format_elapsed(time.monotonic() - stage_started)}"
                    ),
                    force=(idx == total),
                    min_interval=0.1,
                )
            continue

        stem = safe_file_stem(f"{paper.canonical_id}-{idx}")
        last_error: str | None = None
        downloaded_any = False
        extracted_any = False

        for candidate_idx, pdf_url in enumerate(paper.pdf_urls[:max_candidates_per_paper], start=1):
            destination = temp_pdf_dir / f"{stem}-{candidate_idx}.pdf"
            ok, error = download_pdf(client, pdf_url, destination, max_pdf_mb=max_pdf_mb)
            if not ok:
                last_error = error or "PDF download failed"
                continue

            downloaded_any = True
            paper.document_pdf_url = pdf_url
            text, extract_error = extract_text_from_pdf(destination, max_pages=max_pages)
            if extract_error or not text:
                last_error = extract_error or "No extractable PDF text"
                continue

            extracted_any = True
            mentions = find_award_mentions(text, award_regexes)
            if mentions:
                paper.award_mentioned_in_document = True
                paper.document_award_mentions = mentions
                paper.document_award_context = find_award_context(text, mentions)
                paper.award_mentions = _merge_string_lists(paper.award_mentions, mentions)
                paper.award_mentioned_in_metadata = True
            break

        if not downloaded_any and last_error:
            paper.document_scan_error = last_error
            download_fail_count += 1
        elif downloaded_any and not extracted_any and last_error:
            paper.document_scan_error = last_error
            extract_fail_count += 1
        elif extracted_any and not paper.award_mentioned_in_document:
            paper.document_scan_error = None

        if paper.award_mentioned_in_document:
            mention_count += 1

        if status:
            status.update(
                (
                    f"[paper-scan] {_progress_bar(idx, total)} {idx}/{total} "
                    f"| mentions {mention_count} | no_pdf {no_pdf_count} "
                    f"| dl_fail {download_fail_count} | extract_fail {extract_fail_count} "
                    f"| elapsed {_format_elapsed(time.monotonic() - stage_started)}"
                ),
                force=(idx == total),
                min_interval=0.1,
            )

    return {
        "papers_total": total,
        "mentions_count": mention_count,
        "no_pdf_count": no_pdf_count,
        "download_fail_count": download_fail_count,
        "extract_fail_count": extract_fail_count,
    }


def _canonical_id(record: SourcePaperRecord) -> str:
    if record.doi:
        return f"doi:{record.doi.lower()}"

    arxiv_id = _extract_arxiv_id(" ".join(part for part in [record.source_id, record.landing_page_url or "", record.pdf_url or ""] if part))
    if arxiv_id:
        return f"arxiv:{arxiv_id}"

    norm_title = _normalize_title(record.title)
    year = record.year if record.year is not None else "na"
    return f"title:{year}:{norm_title}"


def _extract_arxiv_id(value: str) -> str | None:
    match = re.search(r"([0-9]{4}\.[0-9]{4,5})(?:v\d+)?", value, flags=re.IGNORECASE)
    if match:
        return match.group(1)

    hyphenated = re.search(r"abs[-_/]([0-9]{4})[-_/]([0-9]{4,5})(?:v\d+)?", value, flags=re.IGNORECASE)
    if hyphenated:
        return f"{hyphenated.group(1)}.{hyphenated.group(2)}"
    return None


def _normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _prefer_better_title(current: str, incoming: str) -> str:
    if len(incoming) > len(current):
        return incoming
    return current


def _pick_year(current: int | None, incoming: int | None) -> int | None:
    if current is None:
        return incoming
    if incoming is None:
        return current
    return min(current, incoming)


def _pick_published_date(current: str | None, incoming: str | None) -> str | None:
    if current is None:
        return incoming
    if incoming is None:
        return current
    return min(current, incoming)


def _clean_text(value: str) -> str:
    return " ".join((value or "").split())


def _merge_string_lists(left: list[str], right: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in [*left, *right]:
        cleaned = _clean_text(value)
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _render_report(summary: CollectionRunSummary, papers_with_award: list[CanonicalPaper]) -> str:
    lines: list[str] = []
    lines.append("# Ledger Collection Report")
    lines.append("")
    lines.append(f"Generated: {summary.run_finished_at}")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Team members: {summary.team_member_count}")
    lines.append(f"- Lookback years: {summary.lookback_years}")
    lines.append(f"- Raw source records: {summary.raw_record_count}")
    lines.append(f"- Canonical papers: {summary.canonical_paper_count}")
    lines.append(f"- Award mentions (metadata + document scan): {summary.award_match_count}")
    lines.append(f"- Proxy attempts: {summary.proxy_attempt_count}")
    lines.append(f"- Direct attempts: {summary.direct_attempt_count}")
    lines.append("")
    lines.append("## Source Counts")
    for source, count in sorted(summary.source_record_counts.items()):
        errors = summary.source_error_counts.get(source, 0)
        probe = summary.source_probe_status.get(source, "n/a")
        lines.append(f"- {source}: {count} records ({errors} errors) | probe: {probe}")

    lines.append("")
    lines.append("## Papers With Award Mention")
    if not papers_with_award:
        lines.append("- None")
    else:
        for paper in papers_with_award[:100]:
            year = paper.year if paper.year is not None else "n/a"
            venue = paper.venue or "unknown venue"
            members = ", ".join(paper.aimi_members)
            mentions = ", ".join(paper.award_mentions)
            lines.append(f"- **{paper.title}** ({year}, {venue}) | AIMI member(s): {members} | mention(s): {mentions}")

    lines.append("")
    return "\n".join(lines)


def _normalize_error(error: str) -> str:
    compact = " ".join((error or "").split())
    if "SSLEOFError" in compact or "SSL_connect" in compact:
        return "TLS handshake failed through proxy for this source endpoint."
    if "ProxyError" in compact:
        return "Proxy connection failed for this source endpoint."
    if len(compact) > 240:
        return compact[:237] + "..."
    return compact or "Unknown source error"


def _probe_source_connectivity(
    *,
    source_name: str,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[bool, str]:
    url = _source_probe_url(source_name, config=config, min_year=min_year)
    headers = _source_probe_headers(source_name, config=config)
    prefer = "expedition" if source_name == "google_scholar" else "requests"
    _, error = client.fetch_text(url, headers=headers, prefer=prefer)
    if error:
        return False, _normalize_error(error)
    return True, "ok"


def _source_probe_url(source_name: str, *, config: LedgerConfig, min_year: int) -> str:
    if source_name == "dblp":
        return f"{config.dblp_author_search_api}?q=Kilian%20Weinberger&format=json&h=1"
    if source_name == "openalex":
        return f"{config.openalex_author_search_api}?search=Kilian+Weinberger&per-page=1"
    if source_name == "semantic_scholar":
        return (
            f"{config.semantic_scholar_author_search_api}"
            "?query=Kilian+Weinberger&limit=1&fields=authorId,name"
        )
    if source_name == "crossref":
        return f"{config.crossref_works_api}?query.author=Kilian+Weinberger&rows=1"
    if source_name == "arxiv":
        return (
            f"{config.arxiv_api}"
            "?search_query=all:materials&start=0&max_results=1&sortBy=submittedDate&sortOrder=descending"
        )
    if source_name == "google_scholar":
        if config.serpapi_api_key:
            return (
                f"{config.google_scholar_serpapi_api}"
                f"?engine=google_scholar&q=materials&hl=en&as_ylo={min_year}&api_key={config.serpapi_api_key}"
            )
        return f"{config.google_scholar_search_url}?q=materials&hl=en&as_ylo={min_year}"
    raise KeyError(f"Unknown source for probe: {source_name}")


def _source_probe_headers(source_name: str, *, config: LedgerConfig) -> dict[str, str]:
    if source_name in {"dblp", "openalex", "semantic_scholar", "crossref"}:
        headers = {"Accept": "application/json"}
    elif source_name == "arxiv":
        headers = {"Accept": "application/atom+xml"}
    else:
        headers = {"Accept": "text/html"}
    if source_name == "google_scholar" and config.serpapi_api_key:
        headers = {"Accept": "application/json"}
    if source_name == "semantic_scholar" and config.semantic_scholar_api_key:
        headers["x-api-key"] = config.semantic_scholar_api_key
    return headers


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _progress_bar(completed: int, total: int, *, width: int = 18) -> str:
    if total <= 0:
        return "[" + ("-" * width) + "]"
    bounded = max(0, min(completed, total))
    filled = int(round((bounded / total) * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _format_elapsed(seconds: float) -> str:
    if seconds < 0:
        seconds = 0.0
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    rem = seconds - (minutes * 60)
    return f"{minutes}m{rem:04.1f}s"


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
