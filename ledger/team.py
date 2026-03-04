from __future__ import annotations

import re
from collections import OrderedDict

from bs4 import BeautifulSoup

from .models import Member

NAME_STOPWORDS = {
    "team",
    "faculty",
    "students",
    "postdocs",
    "news",
    "events",
    "contact",
    "opportunities",
    "institute",
    "research",
    "materials",
    "ai-mi",
}

TOKEN_RE = re.compile(r"^[A-Z][A-Za-z'`.-]*$")


def parse_team_members(html: str, source_url: str) -> list[Member]:
    soup = BeautifulSoup(html, "lxml")
    seen: "OrderedDict[str, Member]" = OrderedDict()

    selectors = [
        "h3.et_pb_module_header a",
        "h4.et_pb_module_header a",
        ".et_pb_blurb_container h3 a",
        ".et_pb_blurb_container h4 a",
    ]

    for selector in selectors:
        for node in soup.select(selector):
            name = normalize_whitespace(node.get_text(" ", strip=True))
            if not is_probable_person_name(name):
                continue
            key = canonical_name_key(name)
            if key in seen:
                continue
            seen[key] = Member(
                name=name,
                source_url=source_url,
                profile_url=node.get("href"),
            )

    # Fallback for markup changes: scan anchors with person-like names.
    if not seen:
        for node in soup.find_all("a"):
            name = normalize_whitespace(node.get_text(" ", strip=True))
            if not is_probable_person_name(name):
                continue
            key = canonical_name_key(name)
            if key in seen:
                continue
            seen[key] = Member(
                name=name,
                source_url=source_url,
                profile_url=node.get("href"),
            )

    return list(seen.values())



def normalize_whitespace(text: str) -> str:
    return " ".join(text.split())



def canonical_name_key(name: str) -> str:
    return re.sub(r"[^a-z]", "", name.lower())



def is_probable_person_name(text: str) -> bool:
    if not text:
        return False
    cleaned = normalize_whitespace(text)
    if len(cleaned) < 5 or len(cleaned) > 80:
        return False
    lower = cleaned.lower()
    if any(sw in lower for sw in NAME_STOPWORDS):
        return False

    tokens = cleaned.replace("\u2019", "'").split(" ")
    if len(tokens) < 2 or len(tokens) > 6:
        return False

    valid = 0
    for token in tokens:
        if token.endswith(".") and len(token) == 2 and token[0].isupper():
            valid += 1
            continue
        if TOKEN_RE.match(token):
            valid += 1
            continue
        return False

    return valid == len(tokens)
