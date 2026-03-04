from ledger.funding import compile_award_regexes, find_award_mentions


def test_award_variants_are_detected() -> None:
    regexes = compile_award_regexes(["DMR-2433348", "DMR2433348", "DMR 2433348"])
    text = (
        "This work was supported in part by the NSF AI institute under "
        "Award No. DMR-2433348 and related cooperative agreement DMR 2433348."
    )
    matches = find_award_mentions(text, regexes)
    joined = " ".join(matches)
    assert "2433348" in joined


def test_numeric_only_detection() -> None:
    regexes = compile_award_regexes([])
    text = "Grant reference: 2433348 appears in acknowledgements."
    matches = find_award_mentions(text, regexes)
    assert matches


def test_split_digit_detection() -> None:
    regexes = compile_award_regexes([])
    text = "Supported by NSF grant DMR 2 4 3 3 3 4 8 in this project."
    matches = find_award_mentions(text, regexes)
    assert any("2433348" in match.replace(" ", "") for match in matches)
