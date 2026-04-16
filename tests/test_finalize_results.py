from pathlib import Path

from ledger.finalize_results import (
    FinalManifestItem,
    _compose_metadata_snapshot_text,
    _decode_openalex_abstract,
    _generate_snapshot_pdf,
    _group_papers,
    render_final_markdown,
)


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
