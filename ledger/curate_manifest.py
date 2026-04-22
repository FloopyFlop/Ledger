from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_BASE_MANIFEST = Path("targets/final_results_manifest.json")
DEFAULT_RUN_DIR = Path("output/latest")
LOW_CONFIDENCE_SOURCES = {
    "crossref",
    "datacite",
    "semantic_scholar",
    "google_scholar",
    "openaire",
    "doaj",
}
HIGH_CONFIDENCE_SOURCES = {
    "dblp",
    "openalex",
    "inspirehep",
}
PREPRINT_VENUE_KEYWORDS = {
    "arxiv",
    "biorxiv",
    "medrxiv",
    "chemrxiv",
    "ssrn",
    "preprint",
}
PREPRINT_DOI_PREFIXES = (
    "10.48550/arxiv.",
    "10.1101/",
)


@dataclass(slots=True)
class ReviewDecision:
    accepted: bool
    confidence: str
    reason: str
    portal_eligible: bool
    is_preprint: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ledger-curate-manifest",
        description="Conservatively review candidate final-result entries against canonical source evidence.",
    )
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR, help="Run directory (default: output/latest).")
    parser.add_argument(
        "--base-manifest",
        type=Path,
        default=DEFAULT_BASE_MANIFEST,
        help="Previously approved baseline manifest to preserve and extend.",
    )
    parser.add_argument(
        "--candidate-manifest",
        type=Path,
        required=True,
        help="Candidate additions to review.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("targets"),
        help="Directory for generated reviewed manifests.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    run_dir = args.run_dir.expanduser().resolve()
    papers_path = run_dir / "papers_canonical.json"
    if not papers_path.exists():
        raise RuntimeError(f"Canonical papers file not found: {papers_path}")

    base_manifest = _load_manifest(args.base_manifest)
    candidate_manifest = _load_manifest(args.candidate_manifest)
    papers = _load_json(papers_path)
    if not isinstance(papers, list):
        raise RuntimeError(f"Invalid canonical paper payload in {papers_path}")

    papers_by_title = {_normalize_title(str(paper.get("title") or "")): paper for paper in papers if isinstance(paper, dict)}
    papers_by_doi = {
        str(paper.get("doi") or "").strip().lower(): paper
        for paper in papers
        if isinstance(paper, dict) and str(paper.get("doi") or "").strip()
    }
    base_keys = {_manifest_key(item) for item in base_manifest}

    accepted_additions: list[dict[str, Any]] = []
    rejected_candidates: list[dict[str, Any]] = []
    cumulative_manifest = list(base_manifest)

    for item in candidate_manifest:
        if _manifest_key(item) in base_keys:
            rejected_candidates.append(
                {
                    "title": item.get("title"),
                    "doi": item.get("doi"),
                    "reason": "already present in baseline manifest",
                    "confidence": "baseline",
                    "portal_eligible": _portal_eligible(item, None),
                    "sources": [],
                }
            )
            continue

        canonical = _match_canonical_paper(item, papers_by_doi, papers_by_title)
        decision = assess_candidate(item, canonical)
        record = {
            "title": item.get("title"),
            "doi": item.get("doi"),
            "reason": decision.reason,
            "confidence": decision.confidence,
            "portal_eligible": decision.portal_eligible,
            "is_preprint": decision.is_preprint,
            "sources": sorted(set(canonical.get("sources") or [])) if isinstance(canonical, dict) else [],
        }

        if decision.accepted:
            accepted_additions.append(item)
            cumulative_manifest.append(item)
        else:
            rejected_candidates.append(record)

    portal_manifest = [
        item for item in cumulative_manifest
        if _portal_eligible(item, _match_canonical_paper(item, papers_by_doi, papers_by_title))
    ]

    output_dir = args.output_dir.expanduser().resolve()
    _write_json(output_dir / "accepted_additions_final_results_manifest.json", accepted_additions)
    _write_json(output_dir / "cumulative_final_results_manifest.json", cumulative_manifest)
    _write_json(output_dir / "portal_final_results_manifest.json", portal_manifest)
    _write_json(
        output_dir / "manifest_review_summary.json",
        {
            "base_manifest": str(args.base_manifest.expanduser().resolve()),
            "candidate_manifest": str(args.candidate_manifest.expanduser().resolve()),
            "run_dir": str(run_dir),
            "baseline_count": len(base_manifest),
            "candidate_count": len(candidate_manifest),
            "accepted_addition_count": len(accepted_additions),
            "cumulative_count": len(cumulative_manifest),
            "portal_count": len(portal_manifest),
            "rejected_candidates": rejected_candidates,
        },
    )

    print("Manifest curation finished")
    print(f"Accepted additions: {len(accepted_additions)}")
    print(f"Cumulative manifest: {len(cumulative_manifest)}")
    print(f"Portal manifest: {len(portal_manifest)}")
    print(f"Output dir: {output_dir}")


def assess_candidate(item: dict[str, Any], canonical: dict[str, Any] | None) -> ReviewDecision:
    if canonical is None:
        return ReviewDecision(
            accepted=False,
            confidence="missing",
            reason="candidate not found in canonical papers",
            portal_eligible=False,
            is_preprint=_is_preprint(item, None),
        )

    sources = set(str(source).strip() for source in (canonical.get("sources") or []) if str(source).strip())
    is_preprint = _is_preprint(item, canonical)
    portal_eligible = _portal_eligible(item, canonical)

    if sources & HIGH_CONFIDENCE_SOURCES:
        return ReviewDecision(True, "high", "authoritative source evidence present", portal_eligible, is_preprint)

    if sources == {"arxiv"}:
        return ReviewDecision(True, "high-preprint", "arXiv-only preprint accepted for bibliography", portal_eligible, True)

    if {"pubmed", "europe_pmc"} <= sources:
        return ReviewDecision(True, "high", "PubMed and Europe PMC corroborate the DOI record", portal_eligible, is_preprint)

    if "pubmed" in sources or "europe_pmc" in sources:
        return ReviewDecision(True, "medium", "biomedical index evidence present", portal_eligible, is_preprint)

    if len(sources) >= 2 and not sources.issubset(LOW_CONFIDENCE_SOURCES):
        return ReviewDecision(True, "medium", "multiple non-trivial sources corroborate the paper", portal_eligible, is_preprint)

    if len(sources) >= 2 and {"crossref", "openaire"} <= sources:
        return ReviewDecision(True, "medium", "Crossref and OpenAIRE both contain the paper", portal_eligible, is_preprint)

    return ReviewDecision(
        accepted=False,
        confidence="low",
        reason="single-source or low-confidence source evidence requires manual review",
        portal_eligible=portal_eligible,
        is_preprint=is_preprint,
    )


def _is_preprint(item: dict[str, Any], canonical: dict[str, Any] | None) -> bool:
    doi = str(item.get("doi") or (canonical or {}).get("doi") or "").strip().lower()
    if doi.startswith(PREPRINT_DOI_PREFIXES):
        return True

    venue = str((canonical or {}).get("venue") or "").strip().lower()
    if any(keyword in venue for keyword in PREPRINT_VENUE_KEYWORDS):
        return True

    sources = {str(source).strip().lower() for source in ((canonical or {}).get("sources") or []) if str(source).strip()}
    if sources == {"arxiv"}:
        return True
    return False


def _portal_eligible(item: dict[str, Any], canonical: dict[str, Any] | None) -> bool:
    doi = str(item.get("doi") or (canonical or {}).get("doi") or "").strip()
    return bool(doi) and not _is_preprint(item, canonical)


def _manifest_key(item: dict[str, Any]) -> tuple[str, str]:
    doi = str(item.get("doi") or "").strip().lower()
    return doi, _normalize_title(str(item.get("title") or ""))


def _match_canonical_paper(
    item: dict[str, Any],
    papers_by_doi: dict[str, dict[str, Any]],
    papers_by_title: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    doi = str(item.get("doi") or "").strip().lower()
    if doi and doi in papers_by_doi:
        return papers_by_doi[doi]
    return papers_by_title.get(_normalize_title(str(item.get("title") or "")))


def _normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _load_manifest(path: Path) -> list[dict[str, Any]]:
    payload = _load_json(path.expanduser().resolve())
    if not isinstance(payload, list):
        raise RuntimeError(f"Invalid manifest JSON payload in {path}")
    return [item for item in payload if isinstance(item, dict)]


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")


if __name__ == "__main__":
    main()
