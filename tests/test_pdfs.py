from ledger.models import Publication
from ledger.pdfs import resolve_pdf_candidates



def test_resolve_pdf_candidates_from_arxiv_ee_and_doi() -> None:
    publication = Publication(
        paper_id="paper-1",
        dblp_key="journals/corr/abs-2503-13517",
        title="Example",
        year=2025,
        month="March",
        venue="CoRR",
        publication_type="article",
        authors=["A", "B"],
        ee_urls=["https://doi.org/10.48550/arXiv.2503.13517"],
        doi="10.48550/arXiv.2503.13517",
        dblp_record_url=None,
    )

    candidates = resolve_pdf_candidates(publication)
    assert "https://arxiv.org/pdf/2503.13517.pdf" in candidates
