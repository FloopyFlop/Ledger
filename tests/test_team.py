from ledger.team import is_probable_person_name, parse_team_members


HTML = """
<html>
  <body>
    <h3 class="et_pb_module_header"><a href="https://example.edu/eunah">Eun-Ah Kim</a></h3>
    <h3 class="et_pb_module_header"><a href="https://example.edu/kilian">Kilian Q. Weinberger</a></h3>
    <h3 class="et_pb_module_header"><a href="https://example.edu/team">The AI-MI Team</a></h3>
  </body>
</html>
"""


def test_parse_team_members_extracts_names() -> None:
    members = parse_team_members(HTML, "https://aimi.cornell.edu/team/")
    names = [member.name for member in members]
    assert "Eun-Ah Kim" in names
    assert "Kilian Q. Weinberger" in names
    assert "The AI-MI Team" not in names


def test_person_name_heuristic() -> None:
    assert is_probable_person_name("Leslie M. Schoop")
    assert not is_probable_person_name("AI-MI Team")
