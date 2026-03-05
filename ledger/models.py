from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Member:
    name: str
    source_url: str
    profile_url: str | None = None
    role: str | None = None
    dblp_pid: str | None = None
    dblp_url: str | None = None
    aliases: list[str] = field(default_factory=list)


@dataclass(slots=True)
class Publication:
    paper_id: str
    dblp_key: str
    title: str
    year: int | None
    month: str | None
    venue: str | None
    publication_type: str
    authors: list[str]
    ee_urls: list[str]
    doi: str | None
    dblp_record_url: str | None
    source_pids: list[str] = field(default_factory=list)
    aimi_authors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ProcessedPaper:
    publication: Publication
    is_new: bool
    award_mentioned: bool
    award_mentions: list[str]
    award_context: str | None
    chosen_pdf_url: str | None
    downloaded_pdf: str | None
    pdfa_path: str | None
    pdf_download_error: str | None
    text_extract_error: str | None
    pdfa_error: str | None
    summary: str


@dataclass(slots=True)
class RunSummary:
    run_started_at: str
    run_finished_at: str
    team_member_count: int
    resolved_member_count: int
    unresolved_member_count: int
    openalex_member_count: int
    publication_error_count: int
    examined_paper_count: int
    new_paper_count: int
    processed_paper_count: int
    scan_failure_count: int
    with_award_count: int
    missing_award_count: int
    output_dir: str


@dataclass(slots=True)
class SourcePaperRecord:
    source: str
    source_id: str
    member_name: str
    title: str
    year: int | None
    published_date: str | None
    venue: str | None
    doi: str | None
    authors: list[str] = field(default_factory=list)
    abstract: str | None = None
    landing_page_url: str | None = None
    pdf_url: str | None = None
    relevance_score: float = 0.0
    raw: dict[str, Any] | None = None


@dataclass(slots=True)
class CanonicalPaper:
    canonical_id: str
    title: str
    normalized_title: str
    year: int | None
    published_date: str | None
    venue: str | None
    doi: str | None
    authors: list[str]
    aimi_members: list[str]
    abstract: str | None
    urls: list[str] = field(default_factory=list)
    pdf_urls: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    source_records: list[dict[str, Any]] = field(default_factory=list)
    award_mentioned_in_metadata: bool = False
    award_mentions: list[str] = field(default_factory=list)
    award_mentioned_in_document: bool = False
    document_award_mentions: list[str] = field(default_factory=list)
    document_award_context: str | None = None
    document_pdf_url: str | None = None
    document_scan_error: str | None = None


@dataclass(slots=True)
class CollectionRunSummary:
    run_started_at: str
    run_finished_at: str
    output_dir: str
    team_member_count: int
    lookback_years: int
    source_record_counts: dict[str, int]
    source_error_counts: dict[str, int]
    source_probe_status: dict[str, str]
    raw_record_count: int
    canonical_paper_count: int
    award_match_count: int
    proxy_attempt_count: int
    direct_attempt_count: int



def to_json_dict(value: Any) -> Any:
    if isinstance(value, list):
        return [to_json_dict(item) for item in value]
    if isinstance(value, dict):
        return {str(k): to_json_dict(v) for k, v in value.items()}
    if hasattr(value, "__dataclass_fields__"):
        out: dict[str, Any] = {}
        for key in value.__dataclass_fields__.keys():
            out[key] = to_json_dict(getattr(value, key))
        return out
    return value
