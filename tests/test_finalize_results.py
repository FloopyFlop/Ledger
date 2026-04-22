from pathlib import Path

from ledger.finalize_results import (
    FinalManifestItem,
    _compose_metadata_snapshot_text,
    _crossref_pdf_candidates,
    _decode_openalex_abstract,
    _generate_snapshot_pdf,
    _group_papers,
    _openalex_pdf_candidates,
    _pdf_candidates_for_group,
    _response_pdf_candidates,
    render_final_markdown,
)
from ledger.net import HttpResponse


def test_group_papers_collapses_exact_title_duplicates() -> None:
    papers = [
        {
            "canonical_id": "doi:10.48550/arxiv.2601.07742",
            "title": "PFT: Phonon Fine-tuning for Machine Learned Interatomic Potentials.",
            "doi": "10.48550/arXiv.2601.07742",
            "urls": ["https://openalex.org/W1"],
            "pdf_urls": ["https://arxiv.org/pdf/2601.07742.pdf"],
        },
        {
            "canonical_id": "arxiv:2601.07742",
            "title": "PFT: Phonon Fine-tuning for Machine Learned Interatomic Potentials",
            "doi": None,
            "urls": ["http://arxiv.org/abs/2601.07742v3"],
            "pdf_urls": ["https://arxiv.org/pdf/2601.07742v3"],
        },
    ]

    groups = _group_papers(papers)
    assert len(groups) == 1
    group = next(iter(groups.values()))
    assert group.canonical_ids == ["doi:10.48550/arxiv.2601.07742", "arxiv:2601.07742"]
    assert "https://arxiv.org/pdf/2601.07742.pdf" in group.pdf_urls
    assert "https://arxiv.org/pdf/2601.07742v3" in group.pdf_urls


def test_render_final_markdown_matches_requested_shape() -> None:
    markdown = render_final_markdown(
        [
            {
                "title": "PFT: Phonon Fine-tuning for Machine Learned Interatomic Potentials",
                "authors": ["Teddy Koker", "Abhijeet Gangan"],
                "aimi_authors": ["Tess Smidt"],
                "url": "https://arxiv.org/abs/2601.07742",
                "doi": "10.48550/arXiv.2601.07742",
            }
        ]
    )

    assert markdown == (
        "**PFT: Phonon Fine-tuning for Machine Learned Interatomic Potentials**\n"
        "Authors: Teddy Koker, Abhijeet Gangan | AIMI Authors: Tess Smidt\n"
        "https://arxiv.org/abs/2601.07742\n"
        "10.48550/arXiv.2601.07742\n"
    )


def test_generate_snapshot_pdf_creates_pdf(tmp_path: Path) -> None:
    destination = tmp_path / "snapshot.pdf"
    _generate_snapshot_pdf(
        destination=destination,
        title="Fallback Snapshot",
        authors=["Author One", "Author Two"],
        aimi_authors=["Author One"],
        url="https://example.org/article",
        doi="10.1000/example",
        snapshot_kind="landing_page_text",
        snapshot_source="https://example.org/article",
        text=" ".join(["Accessible article text."] * 200),
    )

    assert destination.exists()
    assert destination.read_bytes().startswith(b"%PDF")


def test_compose_metadata_snapshot_text_includes_group_abstract() -> None:
    papers = [
        {
            "canonical_id": "doi:10.1000/example",
            "title": "Fallback Metadata Example",
            "doi": "10.1000/example",
            "urls": ["https://doi.org/10.1000/example"],
            "abstract": "This is the stored canonical abstract.",
        }
    ]
    group = next(iter(_group_papers(papers).values()))
    item = FinalManifestItem(
        title="Fallback Metadata Example",
        authors=["Author One"],
        aimi_authors=["Author One"],
        url="https://doi.org/10.1000/example",
        doi="10.1000/example",
    )

    text = _compose_metadata_snapshot_text(
        group=group,
        item=item,
        abstracts=group.abstracts,
        metadata_lines=[],
    )

    assert "Title: Fallback Metadata Example" in text
    assert "This is the stored canonical abstract." in text


def test_decode_openalex_abstract_reconstructs_token_order() -> None:
    abstract = _decode_openalex_abstract(
        {
            "Greenhouse": [0],
            "control": [2],
            "diffusion": [3],
            "for": [1],
        }
    )

    assert abstract == "Greenhouse for control diffusion"


def test_crossref_pdf_candidates_extract_pdf_links() -> None:
    message = {
        "link": [
            {"URL": "https://example.org/article.pdf", "content-type": "application/pdf"},
            {"URL": "https://example.org/article", "content-type": "text/html"},
            {"URL": "https://example.org/download/pdf", "content-type": "unspecified"},
        ]
    }

    candidates = _crossref_pdf_candidates(message)

    assert candidates == ["https://example.org/article.pdf", "https://example.org/download/pdf"]


def test_openalex_pdf_candidates_collects_known_locations() -> None:
    work = {
        "open_access": {"oa_url": "https://example.org/landing"},
        "best_oa_location": {
            "pdf_url": "https://example.org/best.pdf",
            "landing_page_url": "https://example.org/best",
        },
        "primary_location": {
            "pdf_url": "https://example.org/primary.pdf",
            "landing_page_url": "https://example.org/primary",
        },
        "locations": [
            {"pdf_url": "https://example.org/location.pdf", "landing_page_url": "https://example.org/location"}
        ],
    }

    candidates = _openalex_pdf_candidates(work)

    assert candidates == [
        "https://example.org/landing",
        "https://example.org/best.pdf",
        "https://example.org/best",
        "https://example.org/primary.pdf",
        "https://example.org/primary",
        "https://example.org/location.pdf",
        "https://example.org/location",
    ]


def test_response_pdf_candidates_extracts_meta_and_anchor_links() -> None:
    response = HttpResponse(
        url="https://example.org/article",
        final_url="https://example.org/article",
        status_code=200,
        headers={},
        body=(
            b"<html><head><meta name='citation_pdf_url' content='/downloads/paper.pdf'></head>"
            b"<body><a href='supplement.pdf'>PDF</a></body></html>"
        ),
        error=None,
        content_type="text/html",
    )

    candidates = _response_pdf_candidates(response)

    assert candidates == [
        "https://example.org/downloads/paper.pdf",
        "https://example.org/supplement.pdf",
    ]


def test_pdf_candidates_for_non_preprint_doi_skip_arxiv_and_supplements() -> None:
    papers = [
        {
            "canonical_id": "doi:10.1000/example",
            "title": "Published Example",
            "doi": "10.1000/example",
            "pdf_urls": [
                "https://arxiv.org/pdf/2501.12345.pdf",
                "https://static-content.springer.com/esm/art%3A10.1000%2Fexample/MediaObjects/example_MOESM1_ESM.pdf",
                "https://publisher.example.org/article.pdf",
            ],
        }
    ]
    group = next(iter(_group_papers(papers).values()))
    item = FinalManifestItem(
        title="Published Example",
        authors=["Author One"],
        aimi_authors=["Author One"],
        url="https://doi.org/10.1000/example",
        doi="10.1000/example",
    )

    candidates = _pdf_candidates_for_group(group, item)

    assert candidates == ["https://publisher.example.org/article.pdf"]
