from ledger.config import _normalize_arxiv_api_url


def test_normalize_arxiv_api_forces_http_for_export_host() -> None:
    value = _normalize_arxiv_api_url("https://export.arxiv.org/api/query", "http://export.arxiv.org/api/query")
    assert value == "http://export.arxiv.org/api/query"


def test_normalize_arxiv_api_accepts_scheme_less_host() -> None:
    value = _normalize_arxiv_api_url("export.arxiv.org/api/query", "http://export.arxiv.org/api/query")
    assert value == "http://export.arxiv.org/api/query"
