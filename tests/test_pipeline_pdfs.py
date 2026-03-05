from ledger.models import CanonicalPaper
from ledger.pipeline import _derive_pdf_candidates_for_canonical, _extract_arxiv_id, _openalex_work_pdf_candidates


def test_extract_arxiv_id_from_dblp_abs_anchor() -> None:
    url = "https://dblp.org/db/journals/corr/corr2601.html#abs-2601-07742"
    assert _extract_arxiv_id(url) == "2601.07742"


def test_derive_pdf_candidates_from_arxiv_doi_and_source_records() -> None:
    paper = CanonicalPaper(
        canonical_id="doi:10.48550/arxiv.2601.07742",
        title="PFT: Phonon Fine-tuning for Machine Learned Interatomic Potentials.",
        normalized_title="pftphononfinetuningformachinelearnedinteratomicpotentials",
        year=2026,
        published_date="2026-01-01",
        venue="CoRR",
        doi="10.48550/arXiv.2601.07742",
        authors=["Tess E. Smidt"],
        aimi_members=["Tess Smidt"],
        abstract=None,
        urls=[
            "https://dblp.org/db/journals/corr/corr2601.html#abs-2601-07742",
            "https://openalex.org/W7123890020",
        ],
        pdf_urls=[],
        sources=["dblp", "openalex"],
        source_records=[
            {
                "source": "dblp",
                "source_id": "doi:10.48550/arxiv.2601.07742",
                "member_name": "Tess Smidt",
                "relevance_score": 1.0,
                "landing_page_url": "https://dblp.org/db/journals/corr/corr2601.html#abs-2601-07742",
                "pdf_url": None,
            },
            {
                "source": "openalex",
                "source_id": "doi:10.48550/arxiv.2601.07742",
                "member_name": "Tess Smidt",
                "relevance_score": 0.95,
                "landing_page_url": "https://openalex.org/W7123890020",
                "pdf_url": None,
            },
        ],
        award_mentioned_in_metadata=False,
        award_mentions=[],
    )

    candidates = _derive_pdf_candidates_for_canonical(paper)
    assert "https://arxiv.org/pdf/2601.07742.pdf" in candidates


def test_openalex_work_pdf_candidates_collects_pdf_urls() -> None:
    work = {
        "best_oa_location": {
            "pdf_url": "https://www.mdpi.com/2313-433X/11/12/430/pdf?version=1764748658",
            "landing_page_url": "https://doi.org/10.3390/jimaging11120430",
        },
        "primary_location": {
            "pdf_url": "https://www.mdpi.com/2313-433X/11/12/430/pdf?version=1764748658",
            "landing_page_url": "https://doi.org/10.3390/jimaging11120430",
        },
    }
    candidates = _openalex_work_pdf_candidates(work)
    assert "https://www.mdpi.com/2313-433X/11/12/430/pdf?version=1764748658" in candidates
