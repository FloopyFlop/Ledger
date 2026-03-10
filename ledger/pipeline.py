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
    collect_datacite_for_member,
    collect_dblp_for_member,
    collect_doaj_for_member,
    collect_europe_pmc_for_member,
    collect_google_scholar_for_member,
    collect_inspirehep_for_members,
    collect_openalex_for_member,
    collect_openaire_for_member,
    collect_pubmed_for_member,
    collect_semantic_scholar_for_member,
)
from .config import LedgerConfig
from .funding import compile_award_regexes, find_award_context, find_award_mentions
from .models import CanonicalPaper, CollectionRunSummary, Member, SourcePaperRecord, to_json_dict
from .net import HttpClient
from .pdfs import (
    convert_pdf_to_pdfa,
    download_pdf,
    ensure_pdfa_copy,
    extract_text_from_pdf,
    safe_file_stem,
)
from .team import parse_team_members

logger = logging.getLogger(__name__)

SOURCE_ORDER = [
    "dblp",
    "openalex",
    "semantic_scholar",
    "crossref",
    "datacite",
    "europe_pmc",
    "pubmed",
    "openaire",
    "doaj",
    "arxiv",
    "inspirehep",
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

    scan_stats = {
        "papers_total": len(canonical_papers),
        "mentions_count": 0,
        "no_pdf_count": 0,
        "download_fail_count": 0,
        "extract_fail_count": 0,
        "pdfa_success_count": 0,
        "pdfa_failure_count": 0,
    }
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
            workers=config.workers,
            convert_to_pdfa=config.convert_award_pdfs_to_pdfa,
            ghostscript_bin=config.ghostscript_bin,
            pdfa_fallback_copy=config.pdfa_fallback_copy,
            status=status,
        )
        status.clear()
        logger.info(
            (
                "Document award scan completed in %s "
                "(papers=%d, mentions=%d, no_pdf=%d, download_fail=%d, extract_fail=%d, pdfa_ok=%d, pdfa_fail=%d)"
            ),
            _format_elapsed(time.monotonic() - scan_started),
            scan_stats["papers_total"],
            scan_stats["mentions_count"],
            scan_stats["no_pdf_count"],
            scan_stats["download_fail_count"],
            scan_stats["extract_fail_count"],
            scan_stats["pdfa_success_count"],
            scan_stats["pdfa_failure_count"],
        )
    canonical_papers.sort(key=lambda paper: ((paper.year or 0), paper.title.lower()), reverse=True)

    papers_with_award = [paper for paper in canonical_papers if paper.award_mentioned_in_metadata]
    papers_without_award = [paper for paper in canonical_papers if not paper.award_mentioned_in_metadata]
    award_document_summary = _build_award_document_summary(canonical_papers)
    target_coverage = _compute_target_doi_coverage(config.target_dois, canonical_papers)

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
        document_scan_enabled=bool(config.scan_pdfs_for_awards),
        document_scan_mentions_count=int(scan_stats["mentions_count"]),
        document_scan_no_pdf_count=int(scan_stats["no_pdf_count"]),
        document_scan_download_fail_count=int(scan_stats["download_fail_count"]),
        document_scan_extract_fail_count=int(scan_stats["extract_fail_count"]),
        document_scan_pdfa_success_count=int(scan_stats["pdfa_success_count"]),
        document_scan_pdfa_failure_count=int(scan_stats["pdfa_failure_count"]),
        target_doi_total=target_coverage["total"],
        target_doi_matched=target_coverage["matched_count"],
        target_doi_missing=target_coverage["missing_count"],
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
    _write_json(run_dir / "award_document_summary.json", award_document_summary)
    if target_coverage["total"] > 0:
        _write_json(run_dir / "target_doi_coverage.json", target_coverage)
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
    _write_json(latest_dir / "award_document_summary.json", award_document_summary)
    if target_coverage["total"] > 0:
        _write_json(latest_dir / "target_doi_coverage.json", target_coverage)
    _write_json(latest_dir / "papers_canonical.json", [to_json_dict(paper) for paper in canonical_papers])
    _write_json(
        latest_dir / "papers_with_award_mention.json",
        [to_json_dict(paper) for paper in papers_with_award],
    )

    logger.info("Ledger pipeline finished in %s", _format_elapsed(time.monotonic() - run_clock_start))
    if target_coverage["total"] > 0:
        logger.info(
            "Target DOI coverage: %d/%d matched (%d missing)",
            target_coverage["matched_count"],
            target_coverage["total"],
            target_coverage["missing_count"],
        )
        if config.fail_on_missing_target_dois and target_coverage["missing_count"] > 0:
            missing = ", ".join(target_coverage["missing"])
            raise RuntimeError(f"Missing target DOI(s): {missing}")

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
    if source_name == "inspirehep":
        status.update(f"[{source_name}] querying affiliation index...", force=True)
        records, errors = collect_inspirehep_for_members(
            members=members,
            client=client,
            config=config,
            min_year=min_year,
        )
        status.clear()
        return records, errors

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
        "datacite": collect_datacite_for_member,
        "europe_pmc": collect_europe_pmc_for_member,
        "pubmed": collect_pubmed_for_member,
        "openaire": collect_openaire_for_member,
        "doaj": collect_doaj_for_member,
        "arxiv": collect_arxiv_for_member,
        "google_scholar": collect_google_scholar_for_member,
    }
    if source_name not in collectors:
        raise KeyError(f"Unknown source collector: {source_name}")
    return collectors[source_name]


def _workers_for_source(source_name: str, default_workers: int) -> int:
    if source_name in {"dblp", "google_scholar", "pubmed"}:
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
    workers: int,
    convert_to_pdfa: bool,
    ghostscript_bin: str,
    pdfa_fallback_copy: bool,
    status: _LiveStatus | None = None,
) -> dict[str, int]:
    temp_pdf_dir = run_dir / "_pdf_cache"
    temp_pdf_dir.mkdir(parents=True, exist_ok=True)

    total = len(papers)
    if total == 0:
        return {
            "papers_total": 0,
            "mentions_count": 0,
            "no_pdf_count": 0,
            "download_fail_count": 0,
            "extract_fail_count": 0,
            "pdfa_success_count": 0,
            "pdfa_failure_count": 0,
        }

    stage_started = time.monotonic()
    mention_count = 0
    no_pdf_count = 0
    download_fail_count = 0
    extract_fail_count = 0
    pdfa_success_count = 0
    pdfa_failure_count = 0
    completed = 0
    scan_workers = max(1, workers)
    with ThreadPoolExecutor(max_workers=scan_workers) as pool:
        futures = {
            pool.submit(
                _scan_single_paper_for_award,
                idx=idx,
                paper=paper,
                client=client,
                temp_pdf_dir=temp_pdf_dir,
                award_regexes=award_regexes,
                max_pdf_mb=max_pdf_mb,
                max_pages=max_pages,
                max_candidates_per_paper=max_candidates_per_paper,
                convert_to_pdfa=convert_to_pdfa,
                ghostscript_bin=ghostscript_bin,
                pdfa_fallback_copy=pdfa_fallback_copy,
            ): idx
            for idx, paper in enumerate(papers, start=1)
        }

        for future in as_completed(futures):
            completed += 1
            idx = futures[future]
            try:
                stats = future.result()
            except Exception as exc:  # pragma: no cover - safety net for parser/network edge cases.
                paper = papers[idx - 1]
                paper.document_scan_error = f"Document scan failed: {exc}"
                stats = {
                    "mentions_count": 0,
                    "no_pdf_count": 0,
                    "download_fail_count": 1,
                    "extract_fail_count": 0,
                    "pdfa_success_count": 0,
                    "pdfa_failure_count": 0,
                }

            mention_count += int(stats["mentions_count"])
            no_pdf_count += int(stats["no_pdf_count"])
            download_fail_count += int(stats["download_fail_count"])
            extract_fail_count += int(stats["extract_fail_count"])
            pdfa_success_count += int(stats["pdfa_success_count"])
            pdfa_failure_count += int(stats["pdfa_failure_count"])

            if status:
                status.update(
                    (
                        f"[paper-scan] {_progress_bar(completed, total)} {completed}/{total} "
                        f"| mentions {mention_count} | no_pdf {no_pdf_count} "
                        f"| dl_fail {download_fail_count} | extract_fail {extract_fail_count} "
                        f"| pdfa_ok {pdfa_success_count} | pdfa_fail {pdfa_failure_count} "
                        f"| elapsed {_format_elapsed(time.monotonic() - stage_started)}"
                    ),
                    force=(completed == total),
                    min_interval=0.1,
                )

    return {
        "papers_total": total,
        "mentions_count": mention_count,
        "no_pdf_count": no_pdf_count,
        "download_fail_count": download_fail_count,
        "extract_fail_count": extract_fail_count,
        "pdfa_success_count": pdfa_success_count,
        "pdfa_failure_count": pdfa_failure_count,
    }


def _scan_single_paper_for_award(
    *,
    idx: int,
    paper: CanonicalPaper,
    client: HttpClient,
    temp_pdf_dir: Path,
    award_regexes,
    max_pdf_mb: int,
    max_pages: int,
    max_candidates_per_paper: int,
    convert_to_pdfa: bool,
    ghostscript_bin: str,
    pdfa_fallback_copy: bool,
) -> dict[str, int]:
    if not paper.pdf_urls:
        paper.document_scan_error = "No PDF candidates"
        return {
            "mentions_count": 0,
            "no_pdf_count": 1,
            "download_fail_count": 0,
            "extract_fail_count": 0,
            "pdfa_success_count": 0,
            "pdfa_failure_count": 0,
        }

    stem = safe_file_stem(f"{paper.canonical_id}-{idx}")
    last_error: str | None = None
    downloaded_any = False
    extracted_any = False
    pdfa_success_count = 0
    pdfa_failure_count = 0

    for candidate_idx, pdf_url in enumerate(paper.pdf_urls[:max_candidates_per_paper], start=1):
        destination = temp_pdf_dir / f"{stem}-{candidate_idx}.pdf"
        ok, error = download_pdf(client, pdf_url, destination, max_pdf_mb=max_pdf_mb)
        if not ok:
            last_error = error or "PDF download failed"
            continue

        downloaded_any = True
        paper.document_pdf_url = pdf_url
        paper.document_pdf_local_path = str(destination)
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
            if convert_to_pdfa:
                pdfa_dir = temp_pdf_dir.parent / "pdfa"
                pdfa_path = pdfa_dir / f"{stem}-{candidate_idx}.pdf"
                ok_pdfa, pdfa_error = convert_pdf_to_pdfa(
                    destination,
                    pdfa_path,
                    ghostscript_bin=ghostscript_bin,
                )
                if ok_pdfa:
                    paper.document_pdfa_path = str(pdfa_path)
                    paper.document_pdfa_error = None
                    pdfa_success_count += 1
                else:
                    if pdfa_fallback_copy:
                        ensure_pdfa_copy(destination, pdfa_path)
                        paper.document_pdfa_path = str(pdfa_path)
                        paper.document_pdfa_error = (
                            f"PDF/A conversion failed, copied original PDF instead: {pdfa_error}"
                        )
                    else:
                        paper.document_pdfa_error = pdfa_error or "PDF/A conversion failed"
                    pdfa_failure_count += 1
        break

    if not downloaded_any and last_error:
        paper.document_scan_error = last_error
        return {
            "mentions_count": 0,
            "no_pdf_count": 0,
            "download_fail_count": 1,
            "extract_fail_count": 0,
            "pdfa_success_count": 0,
            "pdfa_failure_count": 0,
        }
    if downloaded_any and not extracted_any and last_error:
        paper.document_scan_error = last_error
        return {
            "mentions_count": 0,
            "no_pdf_count": 0,
            "download_fail_count": 0,
            "extract_fail_count": 1,
            "pdfa_success_count": 0,
            "pdfa_failure_count": 0,
        }
    if extracted_any and not paper.award_mentioned_in_document:
        paper.document_scan_error = None

    return {
        "mentions_count": 1 if paper.award_mentioned_in_document else 0,
        "no_pdf_count": 0,
        "download_fail_count": 0,
        "extract_fail_count": 0,
        "pdfa_success_count": pdfa_success_count,
        "pdfa_failure_count": pdfa_failure_count,
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


def _build_award_document_summary(papers: list[CanonicalPaper]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for paper in papers:
        if not paper.award_mentioned_in_document:
            continue
        out.append(
            {
                "canonical_id": paper.canonical_id,
                "title": paper.title,
                "year": paper.year,
                "venue": paper.venue,
                "doi": paper.doi,
                "aimi_members": paper.aimi_members,
                "sources": paper.sources,
                "award_mentions": paper.document_award_mentions,
                "award_context": paper.document_award_context,
                "document_pdf_url": paper.document_pdf_url,
                "document_pdf_local_path": paper.document_pdf_local_path,
                "document_pdfa_path": paper.document_pdfa_path,
                "pdfa_conversion_ok": bool(paper.document_pdfa_path and not paper.document_pdfa_error),
                "document_pdfa_error": paper.document_pdfa_error,
                "document_scan_error": paper.document_scan_error,
            }
        )
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
    lines.append(
        "- Document scan: "
        f"{'enabled' if summary.document_scan_enabled else 'disabled'} "
        f"(mentions={summary.document_scan_mentions_count}, "
        f"no_pdf={summary.document_scan_no_pdf_count}, "
        f"download_fail={summary.document_scan_download_fail_count}, "
        f"extract_fail={summary.document_scan_extract_fail_count}, "
        f"pdfa_ok={summary.document_scan_pdfa_success_count}, "
        f"pdfa_fail={summary.document_scan_pdfa_failure_count})"
    )
    if summary.target_doi_total > 0:
        lines.append(
            f"- Target DOI coverage: {summary.target_doi_matched}/{summary.target_doi_total} "
            f"(missing: {summary.target_doi_missing})"
        )
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
    if source_name == "datacite":
        return (
            f"{config.datacite_works_api}"
            "?query=creators.name%3A%22Kilian+Weinberger%22&page%5Bsize%5D=1&page%5Bnumber%5D=1"
        )
    if source_name == "europe_pmc":
        return (
            f"{config.europe_pmc_search_api}"
            "?query=AUTH%3A%22Kilian+Weinberger%22&format=json&pageSize=1&page=1"
        )
    if source_name == "pubmed":
        return (
            f"{config.pubmed_esearch_api}"
            "?db=pubmed&retmode=json&retmax=1&term=Kilian+Weinberger%5BAuthor%5D"
        )
    if source_name == "openaire":
        return f"{config.openaire_publications_api}?keywords=materials&size=1&page=1"
    if source_name == "doaj":
        return f"{config.doaj_articles_api}/materials?page=1&pageSize=1"
    if source_name == "arxiv":
        return (
            f"{config.arxiv_api}"
            "?search_query=all:materials&start=0&max_results=1&sortBy=submittedDate&sortOrder=descending"
        )
    if source_name == "inspirehep":
        max_year = datetime.now(tz=timezone.utc).year
        query = urllib.parse.quote(f"affid:{config.inspirehep_affiliation_id} and date {min_year}->{max_year}")
        return (
            f"{config.inspirehep_literature_api}"
            f"?q={query}&size=1&sort=mostrecent"
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
    if source_name in {
        "dblp",
        "openalex",
        "semantic_scholar",
        "crossref",
        "inspirehep",
        "datacite",
        "europe_pmc",
        "pubmed",
        "doaj",
    }:
        headers = {"Accept": "application/json"}
    elif source_name == "arxiv":
        headers = {"Accept": "application/atom+xml"}
    elif source_name == "openaire":
        headers = {"Accept": "application/xml,text/xml"}
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


def _compute_target_doi_coverage(target_dois: list[str], papers: list[CanonicalPaper]) -> dict[str, object]:
    dedup_targets: list[str] = []
    seen: set[str] = set()
    for value in target_dois:
        cleaned = _clean_text(value).lower()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        dedup_targets.append(cleaned)

    have_by_doi: dict[str, CanonicalPaper] = {}
    for paper in papers:
        if not paper.doi:
            continue
        doi = _clean_text(paper.doi).lower()
        if doi and doi not in have_by_doi:
            have_by_doi[doi] = paper

    matched: list[str] = []
    missing: list[str] = []
    details: list[dict[str, object]] = []

    for doi in dedup_targets:
        paper = have_by_doi.get(doi)
        if paper is None:
            missing.append(doi)
            details.append({"doi": doi, "found": False})
            continue
        matched.append(doi)
        details.append(
            {
                "doi": doi,
                "found": True,
                "title": paper.title,
                "aimi_members": paper.aimi_members,
                "sources": paper.sources,
            }
        )

    return {
        "total": len(dedup_targets),
        "matched_count": len(matched),
        "missing_count": len(missing),
        "matched": matched,
        "missing": missing,
        "details": details,
    }
