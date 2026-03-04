from __future__ import annotations

import json
import re
import urllib.parse

from .models import Member, Publication
from .net import HttpClient



def resolve_member_openalex(
    member: Member,
    client: HttpClient,
    *,
    author_search_api: str,
) -> tuple[str | None, str | None, str | None]:
    query = urllib.parse.quote(member.name)
    url = f"{author_search_api}?search={query}&per-page=15"
    text, error = client.fetch_text(url, headers={"Accept": "application/json"}, prefer="requests")
    if error or not text:
        return None, None, error or "No response"

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, None, f"Invalid OpenAlex JSON: {exc}"

    results = payload.get("results", [])
    if not isinstance(results, list) or not results:
        return None, None, "No OpenAlex author hit"

    ranked = sorted(
        results,
        key=lambda item: _score_author_hit(member.name, str(item.get("display_name", ""))),
        reverse=True,
    )
    best = ranked[0]
    display_name = str(best.get("display_name", "")).strip()
    author_id = str(best.get("id", "")).strip()
    if not author_id:
        return None, None, "OpenAlex author hit missing id"

    score = _score_author_hit(member.name, display_name)
    if score < 18:
        return None, None, f"Low-confidence OpenAlex match (score={score:.2f})"

    return author_id, display_name, None



def fetch_openalex_publications(
    member: Member,
    *,
    openalex_author_id: str,
    client: HttpClient,
    works_api: str,
    min_year: int,
    max_papers: int,
) -> tuple[list[Publication], str | None]:
    publications: list[Publication] = []
    cursor = "*"

    while True:
        filter_expr = f"author.id:{openalex_author_id},from_publication_date:{min_year}-01-01"
        url = (
            f"{works_api}?filter={urllib.parse.quote(filter_expr)}"
            f"&per-page=200&cursor={urllib.parse.quote(cursor)}"
        )
        text, error = client.fetch_text(url, headers={"Accept": "application/json"}, prefer="requests")
        if error or not text:
            return publications, error or "No response"

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            return publications, f"Invalid OpenAlex JSON: {exc}"

        results = payload.get("results", [])
        if not isinstance(results, list):
            return publications, "OpenAlex payload missing results"

        for work in results:
            publication = _work_to_publication(member, work)
            if publication is None:
                continue
            publications.append(publication)
            if max_papers > 0 and len(publications) >= max_papers:
                return publications, None

        next_cursor = payload.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break
        cursor = str(next_cursor)

    return publications, None



def _work_to_publication(member: Member, work: dict) -> Publication | None:
    title = str(work.get("display_name", "")).strip()
    if not title:
        return None

    year_raw = work.get("publication_year")
    try:
        year = int(year_raw) if year_raw is not None else None
    except Exception:
        year = None

    doi = _extract_openalex_doi(work.get("doi"))
    work_id = str(work.get("id", "")).strip()
    paper_id = _canonical_paper_id(doi=doi, work_id=work_id, title=title)

    authors: list[str] = []
    for authorship in work.get("authorships", []) or []:
        name = str((authorship or {}).get("author", {}).get("display_name", "")).strip()
        if name:
            authors.append(name)

    venue = (
        str(((work.get("primary_location") or {}).get("source") or {}).get("display_name", "")).strip()
        or None
    )

    ee_urls = _collect_external_urls(work)

    publication = Publication(
        paper_id=paper_id,
        dblp_key="",
        title=title,
        year=year,
        month=None,
        venue=venue,
        publication_type=str(work.get("type", "article")),
        authors=authors,
        ee_urls=ee_urls,
        doi=doi,
        dblp_record_url=work_id or None,
        source_pids=[f"openalex:{_short_openalex_id(work_id)}"] if work_id else ["openalex:unknown"],
        aimi_authors=[member.name],
    )
    return publication



def _score_author_hit(target_name: str, candidate_name: str) -> float:
    target_tokens = _tokens(target_name)
    candidate_tokens = _tokens(candidate_name)
    if not target_tokens or not candidate_tokens:
        return -100.0

    target_first = target_tokens[0]
    target_last = target_tokens[-1]
    candidate_first = candidate_tokens[0]
    candidate_last = candidate_tokens[-1]

    score = 0.0
    if target_last == candidate_last:
        score += 20.0
    else:
        score -= 30.0

    if target_first == candidate_first:
        score += 20.0
    elif target_first[0] == candidate_first[0]:
        score += 8.0
    else:
        score -= 8.0

    score += len(set(target_tokens).intersection(candidate_tokens)) * 6.0

    if _normalize_name(target_name) == _normalize_name(candidate_name):
        score += 60.0

    return score



def _tokens(value: str) -> list[str]:
    cleaned = re.sub(r"[^A-Za-z ]", " ", value)
    return [token.lower() for token in cleaned.split() if token]



def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z]", "", value.lower())



def _extract_openalex_doi(value: object) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if "doi.org/" in raw.lower():
        parts = re.split(r"doi\.org/", raw, flags=re.IGNORECASE)
        if len(parts) > 1:
            return parts[1]
    if raw.startswith("10."):
        return raw
    return None



def _collect_external_urls(work: dict) -> list[str]:
    urls: list[str] = []

    doi = _extract_openalex_doi(work.get("doi"))
    if doi:
        urls.append(f"https://doi.org/{doi}")

    best_oa = work.get("best_oa_location") or {}
    primary = work.get("primary_location") or {}

    for candidate in [
        best_oa.get("pdf_url"),
        best_oa.get("landing_page_url"),
        primary.get("pdf_url"),
        primary.get("landing_page_url"),
    ]:
        if candidate:
            urls.append(str(candidate).strip())

    # Deduplicate while preserving order.
    out: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out



def _canonical_paper_id(*, doi: str | None, work_id: str, title: str) -> str:
    if doi:
        return f"doi:{doi.lower()}"
    if work_id:
        return f"openalex:{_short_openalex_id(work_id)}"
    return "openalex:" + re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:120]



def _short_openalex_id(raw_id: str) -> str:
    if not raw_id:
        return "unknown"
    value = raw_id.rstrip("/")
    return value.split("/")[-1]
