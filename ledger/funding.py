from __future__ import annotations

import re


SEPARATED_AWARD_NUMBER_RE = re.compile(
    r"2[\s\-_./,:;]*4[\s\-_./,:;]*3[\s\-_./,:;]*3[\s\-_./,:;]*3[\s\-_./,:;]*4[\s\-_./,:;]*8",
    re.IGNORECASE,
)


def compile_award_regexes(patterns: list[str]) -> list[re.Pattern[str]]:
    regexes: list[re.Pattern[str]] = []
    for pattern in patterns:
        raw = pattern.strip()
        if not raw:
            continue

        flexible = re.escape(raw)
        flexible = flexible.replace(r"\ ", r"\s+")
        flexible = flexible.replace(r"\-", r"[-\s]?")
        regexes.append(re.compile(flexible, re.IGNORECASE))

    # Ensure number-only matching is always available.
    regexes.append(re.compile(r"\b2433348\b", re.IGNORECASE))
    regexes.append(SEPARATED_AWARD_NUMBER_RE)

    # Deduplicate by pattern text.
    unique: dict[str, re.Pattern[str]] = {}
    for regex in regexes:
        unique[regex.pattern] = regex
    return list(unique.values())



def find_award_mentions(text: str, regexes: list[re.Pattern[str]]) -> list[str]:
    if not text:
        return []

    mentions: list[str] = []
    for regex in regexes:
        for match in regex.finditer(text):
            value = " ".join(match.group(0).split())
            if value and value not in mentions:
                mentions.append(value)

    # Fallback for OCR/noisy extraction where separators are inconsistent.
    collapsed = _collapse_alnum(text)
    if "dmr2433348" in collapsed and "DMR2433348" not in mentions:
        mentions.append("DMR2433348")
    if "2433348" in collapsed and "2433348" not in mentions:
        mentions.append("2433348")
    return mentions



def find_award_context(text: str, mentions: list[str], *, window: int = 120) -> str | None:
    if not text or not mentions:
        return None

    lower_text = text.lower()
    for mention in mentions:
        idx = lower_text.find(mention.lower())
        if idx < 0:
            continue
        start = max(0, idx - window)
        end = min(len(text), idx + len(mention) + window)
        snippet = " ".join(text[start:end].split())
        return snippet

    # Fallback context around split/separated award digits in OCR text.
    digit_match = SEPARATED_AWARD_NUMBER_RE.search(text)
    if digit_match:
        start = max(0, digit_match.start() - window)
        end = min(len(text), digit_match.end() + window)
        snippet = " ".join(text[start:end].split())
        return snippet
    return None


def _collapse_alnum(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())
