from pathlib import Path

from ledger.funding import compile_award_regexes
from ledger.models import CanonicalPaper
from ledger.net import HttpResponse
from ledger.pipeline import (
    _compute_target_doi_coverage,
    _scan_single_paper_for_award,
    _select_papers_for_document_scan,
)


class _FakeClient:
    def __init__(self, responses: dict[str, HttpResponse]) -> None:
        self._responses = responses

    def fetch(self, url: str, *, headers=None, prefer="auto"):
        response = self._responses.get(url)
        if response is None:
            return HttpResponse(
                url=url,
                final_url=None,
                status_code=None,
                headers={},
                body=b"",
                error=f"Unexpected URL: {url}",
                content_type=None,
            )
        return response


def test_scan_single_paper_uses_pmc_fulltext_xml_when_pdf_is_challenged(tmp_path) -> None:
    paper = CanonicalPaper(
        canonical_id="doi:10.1126/sciadv.ady1167",
        title="Liquid crystal-driven interfacial ordering of colloidal microplastics.",
        normalized_title="liquidcrystaldriveninterfacialorderingofcolloidalmicroplastics",
        year=2025,
        published_date="2025-12-10",
        venue="Science Advances",
        doi="10.1126/sciadv.ady1167",
        authors=["F Mukherjee", "Fengqi You", "NL Abbott"],
        aimi_members=["Fengqi You", "Nicholas Abbott"],
        abstract=None,
        urls=["https://doi.org/10.1126/sciadv.ady1167"],
        pdf_urls=["https://pmc.ncbi.nlm.nih.gov/articles/PMC12700194/pdf/"],
        sources=["europe_pmc"],
        source_records=[
            {
                "source": "europe_pmc",
                "source_id": "10.1126/sciadv.ady1167",
                "member_name": "Fengqi You",
                "relevance_score": 0.8,
                "landing_page_url": "https://doi.org/10.1126/sciadv.ady1167",
                "pdf_url": "https://pmc.ncbi.nlm.nih.gov/articles/PMC12700194/pdf/",
            }
        ],
    )
    fake_client = _FakeClient(
        {
            "https://pmc.ncbi.nlm.nih.gov/articles/PMC12700194/pdf/": HttpResponse(
                url="https://pmc.ncbi.nlm.nih.gov/articles/PMC12700194/pdf/",
                final_url="https://pmc.ncbi.nlm.nih.gov/articles/PMC12700194/pdf/sciadv.ady1167.pdf",
                status_code=200,
                headers={"Content-Type": "text/html; charset=utf-8"},
                body=b"<html><title>Preparing to download ...</title></html>",
                error=None,
                content_type="text/html; charset=utf-8",
            ),
            "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC12700194/fullTextXML": HttpResponse(
                url="https://www.ebi.ac.uk/europepmc/webservices/rest/PMC12700194/fullTextXML",
                final_url="https://www.ebi.ac.uk/europepmc/webservices/rest/PMC12700194/fullTextXML",
                status_code=200,
                headers={"Content-Type": "application/xml"},
                body=(
                    b'<?xml version="1.0"?><article><back><funding-group>'
                    b"<award-group><award-id>DMR-2433348</award-id></award-group>"
                    b"</funding-group></back></article>"
                ),
                error=None,
                content_type="application/xml",
            ),
        }
    )

    stats = _scan_single_paper_for_award(
        idx=1,
        paper=paper,
        client=fake_client,
        temp_pdf_dir=Path(tmp_path),
        award_regexes=compile_award_regexes(["DMR-2433348"]),
        max_pdf_mb=10,
        max_pages=20,
        max_candidates_per_paper=2,
        convert_to_pdfa=False,
        ghostscript_bin="gs",
        pdfa_fallback_copy=False,
    )

    assert stats["mentions_count"] == 1
    assert paper.award_mentioned_in_document is True
    assert paper.document_verification_kind == "europe_pmc_fulltext_xml"
    assert (
        paper.document_verification_url
        == "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC12700194/fullTextXML"
    )
    assert "DMR-2433348" in paper.document_award_mentions


def test_target_doi_coverage_tracks_award_verification() -> None:
    verified = CanonicalPaper(
        canonical_id="doi:10.48550/arxiv.2601.07742",
        title="PFT",
        normalized_title="pft",
        year=2026,
        published_date="2026-01-15",
        venue="arXiv",
        doi="10.48550/arXiv.2601.07742",
        authors=["T Smidt"],
        aimi_members=["Tess Smidt"],
        abstract=None,
        award_mentioned_in_document=True,
        document_verification_kind="pdf",
        document_verification_url="https://arxiv.org/pdf/2601.07742.pdf",
    )
    missing = CanonicalPaper(
        canonical_id="doi:10.3390/jimaging11120430",
        title="Editorial on the Special Issue",
        normalized_title="editorialonthespecialissue",
        year=2025,
        published_date="2025-12-01",
        venue="Journal of Imaging",
        doi="10.3390/jimaging11120430",
        authors=["Z Zhu"],
        aimi_members=["Zhu"],
        abstract=None,
        award_mentioned_in_document=False,
        award_mentioned_in_metadata=False,
        document_scan_error="Response is not a PDF",
    )

    coverage = _compute_target_doi_coverage(
        ["10.48550/arXiv.2601.07742", "10.3390/jimaging11120430"],
        [verified, missing],
    )

    assert coverage["matched_count"] == 2
    assert coverage["award_verified_count"] == 1
    assert coverage["award_verified_missing_count"] == 1
    assert coverage["award_verified_missing"] == ["10.3390/jimaging11120430"]


def test_select_papers_for_document_scan_target_only_filters_by_doi() -> None:
    target = CanonicalPaper(
        canonical_id="doi:10.1126/sciadv.ady1167",
        title="A",
        normalized_title="a",
        year=2025,
        published_date="2025-12-01",
        venue="Science Advances",
        doi="10.1126/sciadv.ady1167",
        authors=[],
        aimi_members=[],
        abstract=None,
    )
    non_target = CanonicalPaper(
        canonical_id="doi:10.48550/arxiv.2601.00001",
        title="B",
        normalized_title="b",
        year=2026,
        published_date="2026-01-01",
        venue="arXiv",
        doi="10.48550/arXiv.2601.00001",
        authors=[],
        aimi_members=[],
        abstract=None,
    )
    selected = _select_papers_for_document_scan(
        papers=[target, non_target],
        target_dois=["10.1126/sciadv.ady1167"],
        target_only=True,
    )
    assert selected == [target]
