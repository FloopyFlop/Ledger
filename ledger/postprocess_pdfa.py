from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from .config import LedgerConfig
from .net import HttpClient
from .pdfs import convert_pdf_to_pdfa, download_pdf, ensure_pdfa_copy, safe_file_stem

ARXIV_RE = re.compile(r"([0-9]{4}\.[0-9]{4,5})(?:v\d+)?", re.IGNORECASE)


def _load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _bool_or_default(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ledger-pdfa-postprocess",
        description="Post-run PDF/A conversion for award-verified papers.",
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"), help="Path to .env file.")
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=Path("output/latest"),
        help="Run directory to post-process (default: output/latest).",
    )
    parser.add_argument(
        "--ghostscript-bin",
        default=None,
        help="Override Ghostscript binary path (default from config).",
    )
    parser.add_argument(
        "--pdfa-fallback-copy",
        default=None,
        help="If true, copy original PDF when PDF/A conversion fails.",
    )
    parser.add_argument(
        "--download-missing-pdfs",
        default="true",
        help="If true, attempt to download local PDFs for award papers missing local files.",
    )
    parser.add_argument(
        "--pdf-download-max-mb",
        type=int,
        default=None,
        help="Max MB for postprocess PDF download attempts (default from config).",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = LedgerConfig.from_env(args.env_file)
    run_dir = args.run_dir.expanduser().resolve()
    award_dir = run_dir / "award_verified"
    papers_path = award_dir / "papers_with_award_mention.json"
    if not papers_path.exists():
        raise RuntimeError(f"Award papers file not found: {papers_path}")

    ghostscript_bin = args.ghostscript_bin or config.ghostscript_bin
    pdfa_fallback_copy = _bool_or_default(args.pdfa_fallback_copy, config.pdfa_fallback_copy)
    download_missing_pdfs = _bool_or_default(args.download_missing_pdfs, True)
    max_pdf_mb = args.pdf_download_max_mb or config.pdf_scan_max_mb

    papers = _load_json(papers_path, default=[])
    if not isinstance(papers, list):
        raise RuntimeError(f"Invalid JSON payload in {papers_path}")

    client = HttpClient(
        proxy=config.proxy,
        timeout_seconds=config.request_timeout_seconds,
        user_agent=config.user_agent,
        expedition_path=config.expedition_path,
    )
    pdf_dir = award_dir / "pdf"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    pdfa_dir = award_dir / "pdfa"
    pdfa_dir.mkdir(parents=True, exist_ok=True)

    updated_by_id: dict[str, dict[str, object]] = {}
    used_stems: set[str] = set()
    converted = 0
    copied = 0
    failed = 0
    skipped_missing_pdf = 0
    downloaded_missing_pdf = 0

    for paper in papers:
        if not isinstance(paper, dict):
            continue
        canonical_id = str(paper.get("canonical_id", "")).strip()
        if not canonical_id:
            continue

        base = safe_file_stem(str(paper.get("doi") or canonical_id))
        stem = base
        suffix = 2
        while stem in used_stems:
            stem = f"{base}_{suffix}"
            suffix += 1
        used_stems.add(stem)

        pdf_local = paper.get("document_pdf_local_path")
        pdf_source = Path(str(pdf_local)).expanduser() if isinstance(pdf_local, str) and pdf_local.strip() else None
        pdf_dest = pdf_dir / f"{stem}.pdf"

        if pdf_source and pdf_source.exists() and pdf_source.resolve() != pdf_dest.resolve():
            ensure_pdfa_copy(pdf_source, pdf_dest)
            pdf_source = pdf_dest

        if (not pdf_source or not pdf_source.exists()) and download_missing_pdfs:
            for candidate in _pdf_candidates_for_paper(paper):
                ok, error = download_pdf(client, candidate, pdf_dest, max_pdf_mb=max_pdf_mb)
                if ok:
                    pdf_source = pdf_dest
                    paper["document_pdf_url"] = candidate
                    downloaded_missing_pdf += 1
                    break
                paper["document_pdfa_error"] = error or "Failed to download PDF candidate"

        if not pdf_source or not pdf_source.exists():
            skipped_missing_pdf += 1
            updated_by_id[canonical_id] = {
                "document_pdf_local_path": str(pdf_source) if pdf_source else None,
                "document_pdf_url": paper.get("document_pdf_url"),
                "document_pdfa_path": paper.get("document_pdfa_path"),
                "document_pdfa_error": paper.get("document_pdfa_error") or "No local PDF available for conversion",
                "pdfa_conversion_ok": False,
            }
            continue

        pdfa_dest = pdfa_dir / f"{stem}.pdf"
        ok, error = convert_pdf_to_pdfa(pdf_source, pdfa_dest, ghostscript_bin=ghostscript_bin)
        if ok:
            converted += 1
            updated_by_id[canonical_id] = {
                "document_pdf_local_path": str(pdf_source),
                "document_pdf_url": paper.get("document_pdf_url"),
                "document_pdfa_path": str(pdfa_dest),
                "document_pdfa_error": None,
                "pdfa_conversion_ok": True,
            }
            continue

        if pdfa_fallback_copy:
            ensure_pdfa_copy(pdf_source, pdfa_dest)
            copied += 1
            updated_by_id[canonical_id] = {
                "document_pdf_local_path": str(pdf_source),
                "document_pdf_url": paper.get("document_pdf_url"),
                "document_pdfa_path": str(pdfa_dest),
                "document_pdfa_error": f"PDF/A conversion failed, copied original PDF instead: {error}",
                "pdfa_conversion_ok": False,
            }
            continue

        failed += 1
        updated_by_id[canonical_id] = {
            "document_pdf_local_path": str(pdf_source),
            "document_pdf_url": paper.get("document_pdf_url"),
            "document_pdfa_path": None,
            "document_pdfa_error": error or "PDF/A conversion failed",
            "pdfa_conversion_ok": False,
        }

    def apply_updates(path: Path) -> None:
        payload = _load_json(path, default=None)
        if payload is None:
            return
        if not isinstance(payload, list):
            return
        changed = False
        for item in payload:
            if not isinstance(item, dict):
                continue
            cid = str(item.get("canonical_id", "")).strip()
            if not cid or cid not in updated_by_id:
                continue
            update = updated_by_id[cid]
            item["document_pdf_local_path"] = update["document_pdf_local_path"]
            item["document_pdf_url"] = update["document_pdf_url"]
            item["document_pdfa_path"] = update["document_pdfa_path"]
            item["document_pdfa_error"] = update["document_pdfa_error"]
            item["pdfa_conversion_ok"] = update["pdfa_conversion_ok"]
            changed = True
        if changed:
            _write_json(path, payload)

    apply_updates(award_dir / "summary.json")
    apply_updates(award_dir / "papers.json")
    apply_updates(award_dir / "papers_with_award_mention.json")
    apply_updates(run_dir / "award_document_summary.json")
    apply_updates(run_dir / "papers_with_award_mention.json")

    print("PDF/A postprocess finished")
    print(f"Run dir: {run_dir}")
    print(f"Ghostscript: {ghostscript_bin}")
    print(f"Download missing PDFs: {download_missing_pdfs}")
    print(f"Downloaded missing PDFs: {downloaded_missing_pdf}")
    print(f"Converted: {converted}")
    print(f"Fallback-copied: {copied}")
    print(f"Failed: {failed}")
    print(f"Skipped (no local PDF): {skipped_missing_pdf}")


def _pdf_candidates_for_paper(paper: dict) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(raw: str | None) -> None:
        if not isinstance(raw, str):
            return
        value = raw.strip()
        if not value:
            return
        if value in seen:
            return
        seen.add(value)
        candidates.append(value)

    add(paper.get("document_pdf_url"))
    for value in paper.get("pdf_urls") or []:
        add(value if isinstance(value, str) else None)
    for value in paper.get("urls") or []:
        add(value if isinstance(value, str) else None)

    source_records = paper.get("source_records")
    if isinstance(source_records, list):
        for record in source_records:
            if not isinstance(record, dict):
                continue
            add(record.get("pdf_url") if isinstance(record.get("pdf_url"), str) else None)
            add(record.get("landing_page_url") if isinstance(record.get("landing_page_url"), str) else None)

    doi = paper.get("doi")
    if isinstance(doi, str):
        match = ARXIV_RE.search(doi)
        if match:
            add(f"https://arxiv.org/pdf/{match.group(1)}.pdf")

    filtered: list[str] = []
    for value in candidates:
        lowered = value.lower()
        if lowered.endswith(".pdf") or "/pdf" in lowered or "arxiv.org/pdf/" in lowered:
            filtered.append(value)
    return filtered


if __name__ == "__main__":
    main()
