from ledger.collectors import _pubmed_author_matches_member


def test_pubmed_author_matching_handles_hyphenated_given_names() -> None:
    assert _pubmed_author_matches_member("Eun-Ah Kim", "Eun Ah Kim")
    assert _pubmed_author_matches_member("Eun-Ah Kim", "EA Kim")
    assert not _pubmed_author_matches_member("Eun-Ah Kim", "Eunsuk Kim")
    assert not _pubmed_author_matches_member("Eun-Ah Kim", "Eunha Kim")

