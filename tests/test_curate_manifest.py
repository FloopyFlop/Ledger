from ledger.curate_manifest import _match_canonical_paper, assess_candidate


def test_assess_candidate_rejects_crossref_only_entry() -> None:
    item = {"title": "Example", "doi": "10.1000/example"}
    canonical = {"sources": ["crossref"], "venue": "Example Journal", "doi": "10.1000/example"}

    decision = assess_candidate(item, canonical)

    assert decision.accepted is False
    assert decision.confidence == "low"
    assert decision.portal_eligible is True


def test_assess_candidate_accepts_pubmed_europe_pmc_entry() -> None:
    item = {"title": "Example", "doi": "10.1000/example"}
    canonical = {
        "sources": ["crossref", "pubmed", "europe_pmc"],
        "venue": "Clin Cancer Res",
        "doi": "10.1000/example",
    }

    decision = assess_candidate(item, canonical)

    assert decision.accepted is True
    assert decision.confidence == "high"
    assert decision.portal_eligible is True


def test_assess_candidate_accepts_arxiv_for_bibliography_only() -> None:
    item = {"title": "Example", "doi": "10.48550/arXiv.2601.12345"}
    canonical = {
        "sources": ["arxiv"],
        "venue": "arXiv",
        "doi": None,
    }

    decision = assess_candidate(item, canonical)

    assert decision.accepted is True
    assert decision.is_preprint is True
    assert decision.portal_eligible is False


def test_match_canonical_paper_prefers_doi_over_title() -> None:
    item = {"title": "Title Without Period", "doi": "10.1000/example"}
    canonical = {"title": "Title Without Period.", "doi": "10.1000/example"}

    matched = _match_canonical_paper(item, {"10.1000/example": canonical}, {})

    assert matched is canonical
