from __future__ import annotations

import argparse
import json
import re
import shutil
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

from .config import LedgerConfig
from .net import HttpClient
from .pdfs import convert_pdf_to_pdfa, download_pdf, ensure_pdfa_copy, safe_file_stem
from .pipeline import _extract_arxiv_id, _extract_searchable_text_from_response

DEFAULT_MANIFEST = Path("targets/final_results_manifest.json")
DEFAULT_RUN_DIR = Path("output/latest")
MIN_SNAPSHOT_TEXT_LENGTH = 400


@dataclass(slots=True)
class FinalManifestItem:
    title: str
    authors: list[str]
    aimi_authors: list[str]
    url: str
    doi: str


@dataclass(slots=True)
class PaperGroup:
    key: str
    records: list[dict[str, Any]] = field(default_factory=list)
    canonical_ids: list[str] = field(default_factory=list)
    titles: list[str] = field(default_factory=list)
    dois: list[str] = field(default_factory=list)
    urls: list[str] = field(default_factory=list)
    pdf_urls: list[str] = field(default_factory=list)
    abstracts: list[str] = field(default_factory=list)
    verification_sources: list[tuple[str, str]] = field(default_factory=list)

    def add_record(self, record: dict[str, Any]) -> None:
        self.records.append(record)
        _merge_strings(self.canonical_ids, [record.get("canonical_id")])
        _merge_strings(self.titles, [_clean_display_text(record.get("title"), strip_period=False)])
        _merge_strings(self.dois, [record.get("doi")])
        _merge_strings(self.urls, record.get("urls") or [])
        _merge_strings(self.pdf_urls, record.get("pdf_urls") or [])
        _merge_strings(self.pdf_urls, [record.get("document_pdf_url")])
        _merge_strings(self.abstracts, [_clean_display_text(record.get("abstract"), strip_period=False)])
        verification_kind = _clean_text(record.get("document_verification_kind"))
        verification_url = _clean_text(record.get("document_verification_url"))
        if verification_kind and verification_url and (verification_kind, verification_url) not in self.verification_sources:
            self.verification_sources.append((verification_kind, verification_url))

        source_records = record.get("source_records")
        if isinstance(source_records, list):
            for source_record in source_records:
                if not isinstance(source_record, dict):
                    continue
                _merge_strings(self.urls, [source_record.get("landing_page_url")])
                _merge_strings(self.pdf_urls, [source_record.get("pdf_url")])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ledger-prepare-final-results",
        description="Prepare strict-deduped final results, Markdown, and PDF/PDF-A outputs.",
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"), help="Path to .env file.")
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=DEFAULT_RUN_DIR,
        help="Run directory to finalize (default: output/latest).",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Manifest JSON describing the final selected papers.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Destination directory for final outputs (default: <run-dir>/final_results).",
    )
    parser.add_argument(
        "--ghostscript-bin",
        default=None,
        help="Override Ghostscript binary path (default from config).",
    )
    parser.add_argument(
        "--pdfa-fallback-copy",
        default="true",
        help="If true, copy the source PDF into the PDF/A path when conversion fails.",
    )
    parser.add_argument(
        "--pdf-download-max-mb",
        type=int,
        default=None,
        help="Max MB when downloading missing PDFs (default from config).",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = LedgerConfig.from_env(args.env_file)
    run_dir = args.run_dir.expanduser().resolve()
    output_dir = (args.output_dir.expanduser().resolve() if args.output_dir else (run_dir / "final_results"))
    papers_path = run_dir / "papers_canonical.json"
    if not papers_path.exists():
        raise RuntimeError(f"Canonical papers file not found: {papers_path}")

    manifest = _load_manifest(args.manifest)
    papers = _load_json(papers_path, default=[])
    if not isinstance(papers, list):
        raise RuntimeError(f"Invalid JSON payload in {papers_path}")

    groups = _group_papers(papers)
    config_max_mb = args.pdf_download_max_mb or config.pdf_scan_max_mb
    ghostscript_bin = args.ghostscript_bin or config.ghostscript_bin
    pdfa_fallback_copy = _bool_or_default(args.pdfa_fallback_copy, True)

    client = HttpClient(
        proxy=config.proxy,
        timeout_seconds=config.request_timeout_seconds,
        user_agent=config.user_agent,
        expedition_path=config.expedition_path,
    )

    pdf_dir = output_dir / "pdf"
    pdfa_dir = output_dir / "pdfa"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    pdfa_dir.mkdir(parents=True, exist_ok=True)

    finalized: list[dict[str, Any]] = []
    missing: list[str] = []

    for item in manifest:
        key = _normalize_title(item.title)
        group = groups.get(key)
        if group is None:
            missing.append(item.title)
            continue

        record = _prepare_group_result(
            client=client,
            config=config,
            group=group,
            item=item,
            pdf_dir=pdf_dir,
            pdfa_dir=pdfa_dir,
            ghostscript_bin=ghostscript_bin,
            max_pdf_mb=config_max_mb,
            pdfa_fallback_copy=pdfa_fallback_copy,
        )
        finalized.append(record)

    if missing:
        raise RuntimeError("Manifest item(s) not found in canonical papers: " + "; ".join(missing))

    markdown = render_final_markdown(finalized)
    (output_dir / "final_results.md").write_text(markdown, encoding="utf-8")

    summary = {
        "generated_at": _utc_now(),
        "run_dir": str(run_dir),
        "output_dir": str(output_dir),
        "manifest_path": str(args.manifest.expanduser().resolve()),
        "strict_dedupe_group_count": len(groups),
        "selected_count": len(finalized),
        "items": finalized,
    }
    _write_json(output_dir / "summary.json", summary)

    print("Final result preparation finished")
    print(f"Run dir: {run_dir}")
    print(f"Output dir: {output_dir}")
    print(f"Selected: {len(finalized)}")
    print(f"Markdown: {output_dir / 'final_results.md'}")
    print(f"PDF dir: {pdf_dir}")
    print(f"PDF/A dir: {pdfa_dir}")


def render_final_markdown(items: list[dict[str, Any]]) -> str:
    blocks: list[str] = []
    for item in items:
        blocks.append(
            "\n".join(
                [
                    f"**{item['title']}**",
                    f"Authors: {', '.join(item['authors'])} | AIMI Authors: {', '.join(item['aimi_authors'])}",
                    item["url"],
                    item["doi"],
                ]
            )
        )
    return "\n\n".join(blocks) + "\n"


def _prepare_group_result(
    *,
    client: HttpClient,
    config: LedgerConfig,
    group: PaperGroup,
    item: FinalManifestItem,
    pdf_dir: Path,
    pdfa_dir: Path,
    ghostscript_bin: str,
    max_pdf_mb: int,
    pdfa_fallback_copy: bool,
) -> dict[str, Any]:
    stem = safe_file_stem(item.doi or item.title)
    pdf_path = pdf_dir / f"{stem}.pdf"
    pdfa_path = pdfa_dir / f"{stem}.pdf"

    pdf_source_kind = None
    pdf_download_url = None
    pdf_error = None

    local_pdf = _find_existing_local_pdf(group)
    if local_pdf is not None:
        shutil.copy2(local_pdf, pdf_path)
        pdf_source_kind = "existing_local_pdf"
    else:
        for candidate in _pdf_candidates_for_group(group, item):
            ok, error = download_pdf(client, candidate, pdf_path, max_pdf_mb=max_pdf_mb)
            if ok:
                pdf_source_kind = "downloaded_pdf"
                pdf_download_url = candidate
                pdf_error = None
                break
            pdf_error = error or "PDF download failed"

    snapshot_source = None
    snapshot_kind = None
    if pdf_source_kind is None:
        snapshot_text, snapshot_kind, snapshot_source = _fetch_snapshot_text(client, group, item)
        if not snapshot_text:
            snapshot_text, snapshot_kind, snapshot_source = _build_metadata_snapshot_text(
                client=client,
                config=config,
                group=group,
                item=item,
            )
        if snapshot_text:
            _generate_snapshot_pdf(
                destination=pdf_path,
                title=item.title,
                authors=item.authors,
                aimi_authors=item.aimi_authors,
                url=item.url,
                doi=item.doi,
                snapshot_kind=snapshot_kind or "text_snapshot",
                snapshot_source=snapshot_source or item.url,
                text=snapshot_text,
            )
            if snapshot_kind in {
                "canonical_record_abstract",
                "crossref_metadata",
                "openalex_metadata",
                "datacite_metadata",
                "citation_snapshot",
            }:
                pdf_source_kind = "generated_metadata_snapshot_pdf"
            else:
                pdf_source_kind = "generated_text_snapshot_pdf"
            pdf_error = None
        else:
            pdf_error = pdf_error or "No local or downloadable PDF and no usable text source was available"

    pdfa_conversion_ok = False
    pdfa_error = None
    reused_existing_pdfa = False

    existing_pdfa = _find_existing_local_pdfa(group)
    if existing_pdfa is not None:
        source_path, source_ok, source_error = existing_pdfa
        shutil.copy2(source_path, pdfa_path)
        pdfa_conversion_ok = source_ok
        pdfa_error = source_error
        reused_existing_pdfa = True
    elif pdf_path.exists():
        ok, error = convert_pdf_to_pdfa(pdf_path, pdfa_path, ghostscript_bin=ghostscript_bin)
        if ok:
            pdfa_conversion_ok = True
        else:
            pdfa_error = error or "PDF/A conversion failed"
            if pdfa_fallback_copy:
                ensure_pdfa_copy(pdf_path, pdfa_path)
            else:
                pdfa_path = None

    matched_title = group.titles[0] if group.titles else item.title
    return {
        "title": item.title,
        "authors": item.authors,
        "aimi_authors": item.aimi_authors,
        "url": item.url,
        "doi": item.doi,
        "matched_title": matched_title,
        "matched_canonical_ids": group.canonical_ids,
        "matched_dois": group.dois,
        "matched_urls": group.urls,
        "pdf_path": str(pdf_path) if pdf_path.exists() else None,
        "pdf_source_kind": pdf_source_kind,
        "pdf_download_url": pdf_download_url,
        "pdf_error": pdf_error,
        "snapshot_source_kind": snapshot_kind,
        "snapshot_source_url": snapshot_source,
        "pdfa_path": str(pdfa_path) if pdfa_path and pdfa_path.exists() else None,
        "pdfa_conversion_ok": pdfa_conversion_ok,
        "pdfa_error": pdfa_error,
        "reused_existing_pdfa": reused_existing_pdfa,
    }


def _pdf_candidates_for_group(group: PaperGroup, item: FinalManifestItem) -> list[str]:
    candidates: list[str] = []
    _merge_strings(candidates, group.pdf_urls)

    for raw in [item.url, item.doi]:
        arxiv_id = _extract_arxiv_id(raw)
        if arxiv_id:
            _merge_strings(candidates, [f"https://arxiv.org/pdf/{arxiv_id}.pdf"])

    clean: list[str] = []
    for candidate in candidates:
        if "none" in candidate.lower():
            continue
        clean.append(candidate)
    return clean


def _fetch_snapshot_text(
    client: HttpClient,
    group: PaperGroup,
    item: FinalManifestItem,
) -> tuple[str | None, str | None, str | None]:
    candidates: list[tuple[str, str]] = []
    for kind, url in group.verification_sources:
        candidates.append((kind, url))

    if not candidates:
        for url in group.urls:
            candidates.append(("landing_page_text", url))
    candidates.append(("manifest_url", item.url))
    if item.doi:
        candidates.append(("doi_landing_page", f"https://doi.org/{item.doi}"))

    seen: set[tuple[str, str]] = set()
    for kind, url in candidates:
        if not url:
            continue
        key = (kind, url)
        if key in seen:
            continue
        seen.add(key)
        response = client.fetch(
            url,
            headers={"Accept": "application/xml,text/xml,text/html,application/xhtml+xml;q=0.9,*/*;q=0.1"},
        )
        if response.error:
            continue
        if response.status_code and response.status_code >= 400:
            continue
        text = _extract_searchable_text_from_response(response)
        if not _is_usable_snapshot_text(text):
            continue
        return text, kind, response.final_url or url
    return None, None, None


def _build_metadata_snapshot_text(
    *,
    client: HttpClient,
    config: LedgerConfig,
    group: PaperGroup,
    item: FinalManifestItem,
) -> tuple[str | None, str | None, str | None]:
    abstracts: list[str] = []
    metadata_lines: list[str] = []
    snapshot_kind: str | None = None
    snapshot_source: str | None = None

    if group.abstracts:
        _merge_strings(abstracts, group.abstracts)
        snapshot_kind = "canonical_record_abstract"
        snapshot_source = "canonical_record_abstract"

    crossref_message = _fetch_crossref_message(client=client, config=config, doi=item.doi)
    if crossref_message:
        abstract = _clean_display_text(crossref_message.get("abstract"), strip_period=False)
        if abstract:
            _merge_strings(abstracts, [abstract])
            if snapshot_kind is None:
                snapshot_kind = "crossref_metadata"
                snapshot_source = f"{config.crossref_works_api}/{urllib.parse.quote(item.doi, safe='')}"
        _merge_strings(
            metadata_lines,
            [
                _crossref_published_date(crossref_message),
                _crossref_container_title(crossref_message),
            ],
        )

    openalex_work = _fetch_openalex_work(client=client, config=config, doi=item.doi)
    if openalex_work:
        abstract = _decode_openalex_abstract(openalex_work.get("abstract_inverted_index"))
        if abstract:
            _merge_strings(abstracts, [abstract])
            if snapshot_kind is None:
                snapshot_kind = "openalex_metadata"
                encoded = urllib.parse.quote(f"https://doi.org/{item.doi}", safe="")
                snapshot_source = f"{config.openalex_works_api}?filter=doi:{encoded}&per-page=1"

        open_access = openalex_work.get("open_access")
        if isinstance(open_access, dict):
            oa_status = _clean_text(str(open_access.get("oa_status") or ""))
            oa_url = _clean_text(str(open_access.get("oa_url") or ""))
            if oa_status:
                _merge_strings(metadata_lines, [f"OpenAlex OA status: {oa_status}"])
            if oa_url:
                _merge_strings(metadata_lines, [f"OpenAlex OA URL: {oa_url}"])

    datacite_item = _fetch_datacite_doi(client=client, config=config, doi=item.doi)
    if datacite_item:
        datacite_abstract = _datacite_abstract_text(datacite_item)
        if datacite_abstract:
            _merge_strings(abstracts, [datacite_abstract])
            if snapshot_kind is None:
                snapshot_kind = "datacite_metadata"
                snapshot_source = f"{config.datacite_works_api}/{urllib.parse.quote(item.doi, safe='')}"

    composed = _compose_metadata_snapshot_text(
        group=group,
        item=item,
        abstracts=abstracts,
        metadata_lines=metadata_lines,
    )
    if not composed:
        return None, None, None
    if snapshot_kind is None:
        snapshot_kind = "citation_snapshot"
        snapshot_source = item.url
    return composed, snapshot_kind, snapshot_source


def _compose_metadata_snapshot_text(
    *,
    group: PaperGroup,
    item: FinalManifestItem,
    abstracts: list[str],
    metadata_lines: list[str],
) -> str:
    lines: list[str] = [
        f"Title: {item.title}",
        f"Authors: {', '.join(item.authors)}",
        f"AIMI Authors: {', '.join(item.aimi_authors)}",
        f"Primary Link: {item.url}",
        f"DOI: {item.doi}",
    ]
    if group.canonical_ids:
        lines.append(f"Matched Canonical IDs: {', '.join(group.canonical_ids)}")
    if group.urls:
        lines.append(f"Known URLs: {', '.join(group.urls)}")
    if group.pdf_urls:
        lines.append(f"Known PDF Candidates: {', '.join(group.pdf_urls)}")
    lines.extend(line for line in metadata_lines if line)

    if abstracts:
        lines.append("Abstract and Metadata:")
        lines.extend(abstracts)
    else:
        lines.append(
            "No full-text PDF could be downloaded through the configured network path. "
            "This snapshot preserves the resolved citation metadata for final result packaging."
        )

    return "\n\n".join(line for line in lines if line).strip()


def _fetch_crossref_message(
    *,
    client: HttpClient,
    config: LedgerConfig,
    doi: str,
) -> dict[str, Any] | None:
    if not doi:
        return None
    url = f"{config.crossref_works_api}/{urllib.parse.quote(doi, safe='')}"
    payload, error = client.fetch_json(url, headers={"Accept": "application/json"}, prefer="requests")
    if error or not isinstance(payload, dict):
        return None
    message = payload.get("message")
    if not isinstance(message, dict):
        return None
    return message


def _fetch_openalex_work(
    *,
    client: HttpClient,
    config: LedgerConfig,
    doi: str,
) -> dict[str, Any] | None:
    if not doi:
        return None
    encoded = urllib.parse.quote(f"https://doi.org/{doi}", safe="")
    url = f"{config.openalex_works_api}?filter=doi:{encoded}&per-page=1"
    payload, error = client.fetch_json(url, headers={"Accept": "application/json"}, prefer="requests")
    if error or not isinstance(payload, dict):
        return None
    rows = payload.get("results")
    if not isinstance(rows, list) or not rows:
        return None
    work = rows[0]
    if not isinstance(work, dict):
        return None
    return work


def _fetch_datacite_doi(
    *,
    client: HttpClient,
    config: LedgerConfig,
    doi: str,
) -> dict[str, Any] | None:
    if not doi:
        return None
    url = f"{config.datacite_works_api}/{urllib.parse.quote(doi, safe='')}"
    payload, error = client.fetch_json(url, headers={"Accept": "application/json"}, prefer="requests")
    if error or not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    return data


def _crossref_published_date(message: dict[str, Any]) -> str:
    for key in ("published-print", "published-online", "published", "issued"):
        raw = message.get(key)
        if not isinstance(raw, dict):
            continue
        parts = raw.get("date-parts")
        if not isinstance(parts, list) or not parts or not isinstance(parts[0], list):
            continue
        values = [str(part) for part in parts[0] if isinstance(part, int)]
        if values:
            return f"Crossref published date: {'-'.join(values)}"
    return ""


def _crossref_container_title(message: dict[str, Any]) -> str:
    titles = message.get("container-title")
    if isinstance(titles, list):
        for raw in titles:
            title = _clean_display_text(raw, strip_period=False)
            if title:
                return f"Crossref venue: {title}"
    return ""


def _decode_openalex_abstract(value: Any) -> str:
    if not isinstance(value, dict):
        return ""

    positions: dict[int, str] = {}
    for token, raw_positions in value.items():
        if not isinstance(token, str) or not isinstance(raw_positions, list):
            continue
        for raw_position in raw_positions:
            if isinstance(raw_position, int) and raw_position >= 0 and raw_position not in positions:
                positions[raw_position] = token

    if not positions:
        return ""
    ordered = [token for _, token in sorted(positions.items())]
    return " ".join(ordered).strip()


def _datacite_abstract_text(data: dict[str, Any]) -> str:
    attributes = data.get("attributes")
    if not isinstance(attributes, dict):
        return ""
    descriptions = attributes.get("descriptions")
    if not isinstance(descriptions, list):
        return ""

    candidates: list[str] = []
    for row in descriptions:
        if not isinstance(row, dict):
            continue
        if str(row.get("descriptionType") or "").lower() != "abstract":
            continue
        text = _clean_display_text(row.get("description"), strip_period=False)
        if text:
            candidates.append(text)
    return "\n\n".join(candidates).strip()


def _generate_snapshot_pdf(
    *,
    destination: Path,
    title: str,
    authors: list[str],
    aimi_authors: list[str],
    url: str,
    doi: str,
    snapshot_kind: str,
    snapshot_source: str,
    text: str,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(destination),
        pagesize=LETTER,
        leftMargin=0.8 * inch,
        rightMargin=0.8 * inch,
        topMargin=0.8 * inch,
        bottomMargin=0.8 * inch,
        title=title,
        author=", ".join(authors),
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "SnapshotTitle",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=16,
        leading=20,
        textColor=HexColor("#0f172a"),
        alignment=TA_CENTER,
        spaceAfter=14,
    )
    meta_style = ParagraphStyle(
        "SnapshotMeta",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=13,
        textColor=HexColor("#1f2937"),
        spaceAfter=6,
    )
    note_style = ParagraphStyle(
        "SnapshotNote",
        parent=styles["BodyText"],
        fontName="Helvetica-Oblique",
        fontSize=9,
        leading=12,
        textColor=HexColor("#475569"),
        spaceAfter=10,
    )
    body_style = ParagraphStyle(
        "SnapshotBody",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9,
        leading=12,
        textColor=HexColor("#111827"),
        spaceAfter=5,
    )

    story: list[Any] = [
        Paragraph(escape(title), title_style),
        Paragraph(f"<b>Authors:</b> {escape(', '.join(authors))}", meta_style),
        Paragraph(f"<b>AIMI Authors:</b> {escape(', '.join(aimi_authors))}", meta_style),
        Paragraph(f"<b>Primary Link:</b> {escape(url)}", meta_style),
        Paragraph(f"<b>DOI:</b> {escape(doi)}", meta_style),
        Paragraph(f"<b>Snapshot Note:</b> {escape(_snapshot_note(snapshot_kind))}", note_style),
        Paragraph(f"<b>Text Source:</b> {escape(snapshot_kind)} | {escape(snapshot_source)}", note_style),
        Spacer(1, 4),
    ]

    for chunk in _chunk_text(text, max_chars=1500):
        story.append(Paragraph(escape(chunk), body_style))

    doc.build(story)


def _find_existing_local_pdf(group: PaperGroup) -> Path | None:
    for record in group.records:
        for key in ("document_pdf_local_path", "document_pdfa_path"):
            value = record.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            path = Path(value).expanduser()
            if path.exists():
                return path
    return None


def _find_existing_local_pdfa(group: PaperGroup) -> tuple[Path, bool, str | None] | None:
    success_candidate: tuple[Path, bool, str | None] | None = None
    fallback_candidate: tuple[Path, bool, str | None] | None = None

    for record in group.records:
        value = record.get("document_pdfa_path")
        if not isinstance(value, str) or not value.strip():
            continue
        path = Path(value).expanduser()
        if not path.exists():
            continue
        source_ok = not bool(record.get("document_pdfa_error"))
        source_error = record.get("document_pdfa_error")
        candidate = (path, source_ok, source_error if isinstance(source_error, str) else None)
        if source_ok:
            success_candidate = candidate
            break
        if fallback_candidate is None:
            fallback_candidate = candidate

    return success_candidate or fallback_candidate


def _group_papers(papers: list[dict[str, Any]]) -> dict[str, PaperGroup]:
    groups: dict[str, PaperGroup] = {}
    for item in papers:
        if not isinstance(item, dict):
            continue
        key = _normalize_title(str(item.get("title") or ""))
        if not key:
            continue
        group = groups.setdefault(key, PaperGroup(key=key))
        group.add_record(item)
    return groups


def _load_manifest(path: Path) -> list[FinalManifestItem]:
    payload = _load_json(path.expanduser().resolve(), default=[])
    if not isinstance(payload, list):
        raise RuntimeError(f"Invalid manifest JSON payload in {path}")

    items: list[FinalManifestItem] = []
    for idx, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise RuntimeError(f"Manifest row {idx} must be an object")
        title = _clean_display_text(item.get("title"))
        url = _clean_text(item.get("url"))
        doi = _clean_text(item.get("doi"))
        authors = [_clean_display_text(value, strip_period=False) for value in item.get("authors") or []]
        aimi_authors = [_clean_display_text(value, strip_period=False) for value in item.get("aimi_authors") or []]
        if not title or not url or not doi or not authors or not aimi_authors:
            raise RuntimeError(f"Manifest row {idx} is missing required fields")
        items.append(
            FinalManifestItem(
                title=title,
                authors=authors,
                aimi_authors=aimi_authors,
                url=url,
                doi=doi,
            )
        )
    return items


def _chunk_text(text: str, *, max_chars: int) -> list[str]:
    words = text.split()
    if not words:
        return []

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for word in words:
        projected = current_len + len(word) + (1 if current else 0)
        if current and projected > max_chars:
            chunks.append(" ".join(current))
            current = [word]
            current_len = len(word)
        else:
            current.append(word)
            current_len = projected
    if current:
        chunks.append(" ".join(current))
    return chunks


def _is_usable_snapshot_text(value: str | None) -> bool:
    if not value:
        return False
    cleaned = " ".join(value.split())
    if len(cleaned) < MIN_SNAPSHOT_TEXT_LENGTH:
        return False
    lowered = cleaned.lower()
    bad_markers = [
        "preparing to download",
        "recaptcha",
        "captcha",
        "access denied",
        "enable javascript",
        "browser check",
    ]
    return not any(marker in lowered for marker in bad_markers)


def _snapshot_note(snapshot_kind: str) -> str:
    if snapshot_kind in {"landing_page_text", "manifest_url", "doi_landing_page"}:
        return "Generated from accessible article text because the source PDF was unavailable through the current proxy/download path."
    if snapshot_kind in {"canonical_record_abstract", "crossref_metadata", "openalex_metadata", "datacite_metadata"}:
        return "Generated from accessible abstract and citation metadata because the source PDF was unavailable through the current proxy/download path."
    return "Generated from citation metadata because the source PDF was unavailable through the current proxy/download path."


def _normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean_display_text(value).lower())


def _clean_display_text(value: Any, *, strip_period: bool = True) -> str:
    if not isinstance(value, str):
        return ""
    text = unescape(value)
    text = re.sub(r"<[^>]+>", "", text).strip()
    text = re.sub(r"\s+", " ", text)
    if strip_period:
        text = text.rstrip(".").strip()
    return text


def _clean_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _merge_strings(target: list[str], values: list[Any]) -> None:
    seen = {value for value in target if isinstance(value, str)}
    for raw in values:
        if not isinstance(raw, str):
            continue
        value = raw.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        target.append(value)


def _load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")


def _bool_or_default(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
