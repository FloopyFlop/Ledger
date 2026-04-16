from ledger.models import CanonicalPaper
from ledger.pipeline import (
    _classify_papers_against_cache,
    _hydrate_cached_award_results,
    _paper_hash,
)
from ledger.state import LedgerState


def _paper(canonical_id: str, title: str, doi: str | None) -> CanonicalPaper:
    return CanonicalPaper(
        canonical_id=canonical_id,
        title=title,
        normalized_title=title.lower().replace(" ", ""),
        year=2026,
        published_date="2026-01-01",
        venue="Test",
        doi=doi,
        authors=["A Author"],
        aimi_members=["A Author"],
        abstract=None,
    )


def test_classify_papers_against_cache_new_changed_unchanged() -> None:
    unchanged = _paper("doi:10.1/unchanged", "Unchanged", "10.1/unchanged")
    changed = _paper("doi:10.1/changed", "Changed new title", "10.1/changed")
    new = _paper("doi:10.1/new", "New", "10.1/new")

    hashes = {
        unchanged.canonical_id: _paper_hash(unchanged),
        changed.canonical_id: _paper_hash(changed),
        new.canonical_id: _paper_hash(new),
    }
    state = LedgerState(
        paper_index={
            unchanged.canonical_id: {"hash": hashes[unchanged.canonical_id]},
            changed.canonical_id: {"hash": "old_hash_value"},
        }
    )

    stats = _classify_papers_against_cache(
        papers=[unchanged, changed, new],
        paper_hashes=hashes,
        state=state,
    )

    assert stats["new_count"] == 1
    assert stats["changed_count"] == 1
    assert stats["unchanged_count"] == 1
    assert new.canonical_id in stats["new_ids"]
    assert changed.canonical_id in stats["changed_ids"]
    assert unchanged.canonical_id in stats["unchanged_ids"]


def test_hydrate_cached_award_results_applies_unchanged_entries() -> None:
    paper = _paper("doi:10.1/abc", "Paper A", "10.1/abc")
    state = LedgerState(
        paper_index={
            paper.canonical_id: {
                "award_mentioned_in_metadata": True,
                "award_mentioned_in_document": True,
                "award_mentions": ["DMR-2433348"],
                "document_award_mentions": ["2433348"],
                "document_verification_kind": "europe_pmc_fulltext_xml",
                "document_verification_url": "https://example.org/fulltext.xml",
            }
        }
    )

    _hydrate_cached_award_results(
        papers=[paper],
        state=state,
        unchanged_ids={paper.canonical_id},
    )

    assert paper.award_mentioned_in_metadata is True
    assert paper.award_mentioned_in_document is True
    assert "DMR-2433348" in paper.award_mentions
    assert "2433348" in paper.document_award_mentions
    assert paper.document_verification_kind == "europe_pmc_fulltext_xml"
