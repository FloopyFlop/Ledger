from __future__ import annotations

import json
import re
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import replace

from .models import Member, Publication
from .net import HttpClient


def resolve_member_dblp(
    member: Member,
    client: HttpClient,
    search_api_url: str,
    *,
    pid_overrides: dict[str, str] | None = None,
    search_cache: dict[str, list[dict]] | None = None,
) -> tuple[Member, str | None]:
    override = _lookup_override(member.name, pid_overrides)
    if override:
        dblp_url = f"https://dblp.org/pid/{override}"
        return replace(member, dblp_pid=override, dblp_url=dblp_url), None

    all_hits: list[dict] = []
    seen_urls: set[str] = set()

    for query in _author_query_variants(member.name):
        hits = _fetch_author_hits(
            query=query,
            client=client,
            search_api_url=search_api_url,
            search_cache=search_cache,
        )
        for hit in hits:
            info_url = str(hit.get("info", {}).get("url", "")).strip()
            if info_url in seen_urls:
                continue
            seen_urls.add(info_url)
            all_hits.append(hit)

    if not all_hits:
        return member, "No DBLP author hit"

    ranked = sorted(
        all_hits,
        key=lambda hit: _score_author_hit(member.name, hit),
        reverse=True,
    )

    best = ranked[0]
    best_score = _score_author_hit(member.name, best)
    if best_score < 25:
        return member, f"Low-confidence DBLP match (score={best_score:.2f})"

    info = best.get("info", {})
    author_name = str(info.get("author", "")).strip()
    member_aliases = list(dict.fromkeys([*member.aliases, author_name]))

    info_url = str(info.get("url", "")).strip()
    pid = _extract_pid(info_url)
    if not pid:
        return member, "Could not parse DBLP pid"

    member = replace(
        member,
        aliases=member_aliases,
        dblp_pid=pid,
        dblp_url=info_url,
    )
    return member, None



def fetch_member_publications(
    member: Member,
    client: HttpClient,
    *,
    pid_xml_template: str,
    max_papers: int,
    min_year: int,
) -> tuple[list[Publication], str | None]:
    if not member.dblp_pid:
        return [], "Member has no DBLP pid"

    url = pid_xml_template.format(pid=member.dblp_pid)
    response = client.fetch(url, headers={"Accept": "application/xml"}, prefer="requests")
    if response.error:
        return [], response.error
    if response.status_code and response.status_code >= 400:
        return [], f"HTTP {response.status_code}"

    try:
        root = ET.fromstring(response.body)
    except ET.ParseError as exc:
        return [], f"Invalid DBLP XML: {exc}"

    publications: list[Publication] = []

    for record in root.findall("r"):
        children = list(record)
        if not children:
            continue
        entry = children[0]

        year = _to_int(_node_text(entry.find("year")))
        if year is not None and year < min_year:
            continue

        dblp_key = str(entry.attrib.get("key", "")).strip()
        title = _node_text(entry.find("title")) or "(untitled)"
        authors = [_node_text(author) for author in entry.findall("author")]
        authors = [name for name in authors if name]
        ee_urls = [_node_text(ee) for ee in entry.findall("ee")]
        ee_urls = [x for x in ee_urls if x]

        venue = (
            _node_text(entry.find("journal"))
            or _node_text(entry.find("booktitle"))
            or _node_text(entry.find("publisher"))
            or None
        )

        doi = _extract_doi(ee_urls)
        dblp_record_url = _node_text(entry.find("url"))
        if dblp_record_url and not dblp_record_url.startswith("http"):
            dblp_record_url = f"https://dblp.org/{dblp_record_url}"

        paper_id = _canonical_paper_id(
            doi=doi,
            source_key=dblp_key,
            title=title,
            authors=authors,
            year=year,
        )
        publication = Publication(
            paper_id=paper_id,
            dblp_key=dblp_key,
            title=title,
            year=year,
            month=_node_text(entry.find("month")),
            venue=venue,
            publication_type=entry.tag,
            authors=authors,
            ee_urls=ee_urls,
            doi=doi,
            dblp_record_url=dblp_record_url,
            source_pids=[member.dblp_pid],
            aimi_authors=[member.name],
        )
        publications.append(publication)

        if max_papers > 0 and len(publications) >= max_papers:
            break

    return publications, None



def merge_publications(existing: Publication, incoming: Publication) -> Publication:
    source_pids = list(dict.fromkeys([*existing.source_pids, *incoming.source_pids]))
    aimi_authors = list(dict.fromkeys([*existing.aimi_authors, *incoming.aimi_authors]))
    ee_urls = list(dict.fromkeys([*existing.ee_urls, *incoming.ee_urls]))

    doi = existing.doi or incoming.doi
    dblp_key = existing.dblp_key or incoming.dblp_key
    dblp_record_url = existing.dblp_record_url or incoming.dblp_record_url

    return replace(
        existing,
        source_pids=source_pids,
        aimi_authors=aimi_authors,
        ee_urls=ee_urls,
        doi=doi,
        dblp_key=dblp_key,
        dblp_record_url=dblp_record_url,
    )



def _lookup_override(name: str, pid_overrides: dict[str, str] | None) -> str | None:
    if not pid_overrides:
        return None
    key = _normalize_name(name)
    return pid_overrides.get(key)



def _fetch_author_hits(
    *,
    query: str,
    client: HttpClient,
    search_api_url: str,
    search_cache: dict[str, list[dict]] | None,
) -> list[dict]:
    cache_key = query.strip().lower()
    if search_cache is not None and cache_key in search_cache:
        return search_cache[cache_key]

    encoded = urllib.parse.quote(query)
    url = f"{search_api_url}?q={encoded}&format=json&h=10"
    text, error = client.fetch_text(
        url,
        headers={"Accept": "application/json"},
        prefer="requests",
    )
    if error or not text:
        if search_cache is not None:
            search_cache[cache_key] = []
        return []

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        if search_cache is not None:
            search_cache[cache_key] = []
        return []

    hits_data = payload.get("result", {}).get("hits", {}).get("hit", [])
    hits = _as_list(hits_data)
    if search_cache is not None:
        search_cache[cache_key] = hits
    return hits



def _author_query_variants(name: str) -> list[str]:
    tokens = [token for token in _name_tokens_in_order(name) if token]
    if not tokens:
        return [name]

    first = tokens[0]
    last = tokens[-1]
    middle = tokens[1:-1]

    variants = [name]
    variants.append(" ".join(tokens))

    # First/last only is often enough for DBLP when initials vary.
    variants.append(f"{first} {last}")

    # Initial-based forms (e.g., Kin Fai Mak -> K F Mak).
    initials = [token[0] for token in tokens[:-1] if token]
    if initials:
        variants.append(" ".join(initials + [last]))
        variants.append(" ".join([initials[0], last]))

    # Keep middle name tokens when available.
    if middle:
        variants.append(" ".join([first, *middle, last]))

    # Deduplicate and keep stable order.
    cleaned: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        v = " ".join(variant.split())
        if not v:
            continue
        key = v.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(v)
    return cleaned



def _as_list(value: object) -> list[dict]:
    if value is None:
        return []
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if isinstance(value, dict):
        return [value]
    return []



def _score_author_hit(target_name: str, hit: dict) -> float:
    info = hit.get("info", {})
    candidate = str(info.get("author", "")).strip()
    if not candidate:
        return float("-inf")

    target_tokens = _name_tokens(target_name)
    candidate_tokens = _name_tokens(candidate)

    target_last = _last_name(target_name)
    candidate_last = _last_name(candidate)

    score = 0.0
    if target_last and candidate_last and target_last == candidate_last:
        score += 25.0
    else:
        score -= 40.0

    target_first = _first_name(target_name)
    candidate_first = _first_name(candidate)
    if target_first and candidate_first:
        if target_first == candidate_first:
            score += 30.0
        elif target_first[0] == candidate_first[0]:
            score += 12.0
        else:
            score -= 12.0

    overlap = len(target_tokens.intersection(candidate_tokens))
    score += overlap * 8.0

    if _normalize_name(target_name) == _normalize_name(candidate):
        score += 100.0

    # Prefer results where DBLP exposes affiliation notes (usually curated authors).
    if info.get("notes"):
        score += 2.0

    return score



def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z]", "", value.lower())



def _name_tokens(value: str) -> set[str]:
    return set(_name_tokens_in_order(value))



def _name_tokens_in_order(value: str) -> list[str]:
    cleaned = re.sub(r"[^A-Za-z ]", " ", value)
    return [token.lower() for token in cleaned.split() if token]



def _first_name(value: str) -> str:
    tokens = _name_tokens_in_order(value)
    return tokens[0] if tokens else ""



def _last_name(value: str) -> str:
    tokens = _name_tokens_in_order(value)
    return tokens[-1] if tokens else ""



def _extract_pid(dblp_url: str) -> str | None:
    marker = "/pid/"
    if marker not in dblp_url:
        return None
    part = dblp_url.split(marker, 1)[1]
    return part.strip("/") or None



def _node_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    text = "".join(node.itertext())
    return " ".join(text.split())



def _to_int(value: str) -> int | None:
    try:
        return int(value)
    except Exception:
        return None



def _extract_doi(ee_urls: list[str]) -> str | None:
    for url in ee_urls:
        cleaned = url.strip()
        if "doi.org/" in cleaned:
            return cleaned.split("doi.org/", 1)[1]
        if cleaned.startswith("10."):
            return cleaned
    return None



def _canonical_paper_id(
    *,
    doi: str | None,
    source_key: str,
    title: str,
    authors: list[str],
    year: int | None,
) -> str:
    if doi:
        return f"doi:{doi.lower()}"
    if source_key:
        return source_key
    return _fallback_paper_id(title=title, authors=authors, year=year)



def _fallback_paper_id(*, title: str, authors: list[str], year: int | None) -> str:
    author_head = "_".join(authors[:3]).lower()
    year_part = str(year or "na")
    compact_title = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return f"fallback:{year_part}:{compact_title}:{author_head}"[:240]
