from pathlib import Path

from ledger.config import _normalize_arxiv_api_url, _parse_doi_list, _load_target_dois


def test_normalize_arxiv_api_forces_http_for_export_host() -> None:
    value = _normalize_arxiv_api_url("https://export.arxiv.org/api/query", "http://export.arxiv.org/api/query")
    assert value == "http://export.arxiv.org/api/query"


def test_normalize_arxiv_api_accepts_scheme_less_host() -> None:
    value = _normalize_arxiv_api_url("export.arxiv.org/api/query", "http://export.arxiv.org/api/query")
    assert value == "http://export.arxiv.org/api/query"


def test_parse_doi_list_handles_mixed_separators() -> None:
    values = _parse_doi_list(
        "10.48550/arXiv.2601.07742,\n10.1126/sciadv.ady1167 10.1126/sciadv.ady1167"
    )
    assert values == ["10.48550/arXiv.2601.07742", "10.1126/sciadv.ady1167"]


def test_load_target_dois_merges_env_and_file(tmp_path, monkeypatch) -> None:
    doi_file = tmp_path / "targets.txt"
    doi_file.write_text("10.1021/acs.jpcc.5c05925\n10.1126/sciadv.ady1167", encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text("", encoding="utf-8")

    monkeypatch.setenv("LEDGER_TARGET_DOIS", "10.48550/arXiv.2601.07742,10.1126/sciadv.ady1167")
    monkeypatch.setenv("LEDGER_TARGET_DOI_FILE", str(Path("targets.txt")))

    values = _load_target_dois(env_file)
    assert values == [
        "10.48550/arXiv.2601.07742",
        "10.1126/sciadv.ady1167",
        "10.1021/acs.jpcc.5c05925",
    ]
