from pathlib import Path

from ledger.models import Publication
from ledger.pdfs import _render_pdfa_def, resolve_pdf_candidates, validate_pdfa



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


def test_validate_pdfa_rejects_missing_markers(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("ledger.pdfs.shutil.which", lambda _: None)
    path = tmp_path / "plain.pdf"
    path.write_bytes(b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\n")

    ok, error = validate_pdfa(path)

    assert ok is False
    assert error is not None
    assert "pdfaid:part" in error


def test_validate_pdfa_accepts_marker_fallback(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("ledger.pdfs.shutil.which", lambda _: None)
    path = tmp_path / "pdfa.pdf"
    path.write_bytes(
        b"%PDF-1.7\n"
        b"<rdf:Description pdfaid:part='2' pdfaid:conformance='B'/>\n"
        b"/OutputIntents [ 5 0 R ]\n"
    )

    ok, error = validate_pdfa(path)

    assert ok is True
    assert error is None


def test_render_pdfa_def_rewrites_icc_directive_not_comment(tmp_path: Path) -> None:
    template = tmp_path / "PDFA_def.ps"
    template.write_text(
        "% comment (srgb.icc)\n"
        "/ICCProfile (srgb.icc) % Customise\n"
        "def\n",
        encoding="utf-8",
    )

    rendered = _render_pdfa_def(template, Path("/tmp/profile.icc"))

    assert "% comment (srgb.icc)" in rendered
    assert "/ICCProfile (/tmp/profile.icc) % Customise" in rendered
