from ledger.dblp import _author_query_variants, _score_author_hit



def test_query_variants_cover_initial_form() -> None:
    variants = _author_query_variants("Kin Fai Mak")
    lowered = {v.lower() for v in variants}
    assert "k f mak" in lowered



def test_scoring_prefers_correct_first_name() -> None:
    target = "Keith Brown"
    wrong = {"info": {"author": "Anthony Brown 0002"}}
    right = {"info": {"author": "Keith Brown"}}

    assert _score_author_hit(target, right) > _score_author_hit(target, wrong)
