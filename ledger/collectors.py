from __future__ import annotations

import hashlib
import re
import urllib.parse
import xml.etree.ElementTree as ET
from typing import Callable

from bs4 import BeautifulSoup

from .config import LedgerConfig
from .dblp import fetch_member_publications, resolve_member_dblp
from .models import Member, SourcePaperRecord
from .net import HttpClient
from .openalex import fetch_openalex_publications, resolve_member_openalex

ARXIV_ID_RE = re.compile(r"arxiv\.org/(?:abs|pdf)/([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)", re.IGNORECASE)
YEAR_RE = re.compile(r"(19|20)\d{2}")
TAG_RE = re.compile(r"<[^>]+>")


SourceCollector = Callable[[Member, HttpClient, LedgerConfig, int], tuple[list[SourcePaperRecord], str | None]]


def collect_dblp_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
    *,
    search_cache: dict[str, list[dict]] | None = None,
) -> tuple[list[SourcePaperRecord], str | None]:
    resolved_member, resolve_error = resolve_member_dblp(
        member,
        client,
        config.dblp_author_search_api,
        pid_overrides=config.dblp_pid_overrides,
        search_cache=search_cache,
    )
    if resolve_error:
        return [], resolve_error

    publications, fetch_error = fetch_member_publications(
        resolved_member,
        client,
        pid_xml_template=config.dblp_pid_xml_template,
        max_papers=config.max_results_per_member_per_source,
        min_year=min_year,
    )
    if fetch_error:
        return [], fetch_error

    records: list[SourcePaperRecord] = []
    for publication in publications:
        landing_url = publication.dblp_record_url or (publication.ee_urls[0] if publication.ee_urls else None)
        records.append(
            SourcePaperRecord(
                source="dblp",
                source_id=publication.paper_id,
                member_name=member.name,
                title=publication.title,
                year=publication.year,
                published_date=_year_to_iso(publication.year),
                venue=publication.venue,
                doi=publication.doi,
                authors=list(publication.authors),
                abstract=None,
                landing_page_url=landing_url,
                pdf_url=_pick_pdf_url(publication.ee_urls),
                relevance_score=1.0,
                raw=({"dblp_key": publication.dblp_key, "ee_urls": publication.ee_urls} if config.include_raw_payloads else None),
            )
        )
    return records, None


def collect_openalex_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    author_id, _, resolve_error = resolve_member_openalex(
        member,
        client,
        author_search_api=config.openalex_author_search_api,
    )
    if resolve_error or not author_id:
        return [], resolve_error or "No OpenAlex author id"

    publications, fetch_error = fetch_openalex_publications(
        member,
        openalex_author_id=author_id,
        client=client,
        works_api=config.openalex_works_api,
        min_year=min_year,
        max_papers=config.max_results_per_member_per_source,
    )
    if fetch_error:
        return [], fetch_error

    records: list[SourcePaperRecord] = []
    for publication in publications:
        landing_url = publication.dblp_record_url or (publication.ee_urls[0] if publication.ee_urls else None)
        records.append(
            SourcePaperRecord(
                source="openalex",
                source_id=publication.paper_id,
                member_name=member.name,
                title=publication.title,
                year=publication.year,
                published_date=_year_to_iso(publication.year),
                venue=publication.venue,
                doi=publication.doi,
                authors=list(publication.authors),
                abstract=None,
                landing_page_url=landing_url,
                pdf_url=_pick_pdf_url(publication.ee_urls),
                relevance_score=0.95,
                raw=(
                    {
                        "openalex_author_id": author_id,
                        "source_pids": publication.source_pids,
                        "ee_urls": publication.ee_urls,
                    }
                    if config.include_raw_payloads
                    else None
                ),
            )
        )
    return records, None


def collect_semantic_scholar_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    query = urllib.parse.quote(member.name)
    search_url = (
        f"{config.semantic_scholar_author_search_api}?query={query}&limit=10"
        "&fields=authorId,name,paperCount"
    )
    headers = {"Accept": "application/json"}
    if config.semantic_scholar_api_key:
        headers["x-api-key"] = config.semantic_scholar_api_key

    payload, search_error = client.fetch_json(search_url, headers=headers, prefer="requests")
    if search_error or not isinstance(payload, dict):
        return [], search_error or "Invalid Semantic Scholar search payload"

    candidates = payload.get("data") or []
    if not isinstance(candidates, list) or not candidates:
        return [], "No Semantic Scholar author hit"

    best = max(candidates, key=lambda item: _score_author_name(member.name, str(item.get("name", ""))))
    author_id = str(best.get("authorId", "")).strip()
    if not author_id:
        return [], "Semantic Scholar author id missing"

    records: list[SourcePaperRecord] = []
    batch_size = 100
    offset = 0
    max_results = config.max_results_per_member_per_source

    while offset < max_results:
        papers_url = (
            f"{config.semantic_scholar_author_papers_api_template.format(author_id=author_id)}"
            f"?limit={batch_size}&offset={offset}"
            "&fields=paperId,title,year,venue,publicationDate,authors,externalIds,url,openAccessPdf,abstract"
        )
        paper_payload, paper_error = client.fetch_json(papers_url, headers=headers, prefer="requests")
        if paper_error:
            if records:
                return records, None
            return [], paper_error
        if not isinstance(paper_payload, dict):
            break

        rows = paper_payload.get("data") or []
        if not isinstance(rows, list) or not rows:
            break

        for row in rows:
            title = _clean_text(str(row.get("title", "")))
            if not title:
                continue
            year = _safe_int(row.get("year"))
            if year is not None and year < min_year:
                continue

            authors = [
                _clean_text(str(author.get("name", "")))
                for author in (row.get("authors") or [])
                if isinstance(author, dict)
            ]
            authors = [author for author in authors if author]

            external_ids = row.get("externalIds") if isinstance(row.get("externalIds"), dict) else {}
            doi = _normalize_doi(str(external_ids.get("DOI", "")) if external_ids else None)
            paper_id = _clean_text(str(row.get("paperId", ""))) or _stable_id("semantic", title, year)

            url = _clean_optional(str(row.get("url", "")))
            open_access = row.get("openAccessPdf") if isinstance(row.get("openAccessPdf"), dict) else {}
            pdf_url = _clean_optional(str((open_access or {}).get("url", "")))
            venue = _clean_optional(str(row.get("venue", "")))
            published_date = _clean_optional(str(row.get("publicationDate", "")))
            abstract = _clean_optional(str(row.get("abstract", "")))

            records.append(
                SourcePaperRecord(
                    source="semantic_scholar",
                    source_id=paper_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=published_date,
                    venue=venue,
                    doi=doi,
                    authors=authors,
                    abstract=abstract,
                    landing_page_url=url,
                    pdf_url=pdf_url,
                    relevance_score=0.93,
                    raw=(row if config.include_raw_payloads else None),
                )
            )
            if len(records) >= max_results:
                return records, None

        if len(rows) < batch_size:
            break
        offset += batch_size

    return records, None


def collect_crossref_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    rows = min(100, config.max_results_per_member_per_source)
    offset = 0
    records: list[SourcePaperRecord] = []

    while offset < config.max_results_per_member_per_source:
        query = {
            "query.author": member.name,
            "rows": str(rows),
            "offset": str(offset),
            "filter": f"from-pub-date:{min_year}-01-01",
        }
        if config.crossref_mailto:
            query["mailto"] = config.crossref_mailto

        url = f"{config.crossref_works_api}?{urllib.parse.urlencode(query)}"
        payload, error = client.fetch_json(url, headers={"Accept": "application/json"}, prefer="requests")
        if error:
            if records:
                return records, None
            return [], error
        if not isinstance(payload, dict):
            break

        message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
        items = message.get("items") if isinstance(message, dict) else []
        if not isinstance(items, list) or not items:
            break

        for item in items:
            if not isinstance(item, dict):
                continue

            title_list = item.get("title") or []
            title = _clean_text(str(title_list[0])) if isinstance(title_list, list) and title_list else ""
            if not title:
                continue

            authors = _crossref_authors(item.get("author") or [])
            if authors and not any(_author_matches_member(member.name, author) for author in authors):
                continue

            year = _crossref_year(item)
            if year is not None and year < min_year:
                continue

            doi = _normalize_doi(str(item.get("DOI", "")))
            venue = _crossref_container(item)
            published_date = _crossref_date(item)
            abstract = _strip_tags(_clean_optional(str(item.get("abstract", ""))))
            landing_page = _clean_optional(str(item.get("URL", "")))
            pdf_url = _crossref_pdf(item.get("link") or [])
            source_id = doi or _stable_id("crossref", title, year)

            records.append(
                SourcePaperRecord(
                    source="crossref",
                    source_id=source_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=published_date,
                    venue=venue,
                    doi=doi,
                    authors=authors,
                    abstract=abstract,
                    landing_page_url=landing_page,
                    pdf_url=pdf_url,
                    relevance_score=0.85,
                    raw=(item if config.include_raw_payloads else None),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        if len(items) < rows:
            break
        offset += rows

    return records, None


def collect_arxiv_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    records: list[SourcePaperRecord] = []
    start = 0
    batch_size = min(100, config.max_results_per_member_per_source)

    while start < config.max_results_per_member_per_source:
        query = f'au:"{member.name}"'
        params = {
            "search_query": query,
            "start": str(start),
            "max_results": str(batch_size),
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        url = f"{config.arxiv_api}?{urllib.parse.urlencode(params)}"
        xml_text, error = client.fetch_text(url, headers={"Accept": "application/atom+xml"}, prefer="requests")
        if error or not xml_text:
            if records:
                return records, None
            return [], error or "No arXiv response"

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            if records:
                return records, None
            return [], f"Invalid arXiv XML: {exc}"

        ns = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
        entries = root.findall("atom:entry", ns)
        if not entries:
            break

        for entry in entries:
            title = _clean_text(_xml_text(entry.find("atom:title", ns)))
            if not title:
                continue

            published = _clean_optional(_xml_text(entry.find("atom:published", ns)))
            year = _year_from_text(published)
            if year is not None and year < min_year:
                continue

            authors = [
                _clean_text(_xml_text(node))
                for node in entry.findall("atom:author/atom:name", ns)
            ]
            authors = [author for author in authors if author]
            if authors and not any(_author_matches_member(member.name, author) for author in authors):
                continue

            abstract = _clean_optional(_clean_text(_xml_text(entry.find("atom:summary", ns))))
            landing_page = _clean_optional(_xml_text(entry.find("atom:id", ns)))
            doi = _normalize_doi(_xml_text(entry.find("arxiv:doi", ns)))
            pdf_url = None
            for link in entry.findall("atom:link", ns):
                title_attr = _clean_optional(link.attrib.get("title"))
                href = _clean_optional(link.attrib.get("href"))
                if title_attr == "pdf" and href:
                    pdf_url = href
                    break
            if not pdf_url and landing_page:
                arxiv_id = _extract_arxiv_id(landing_page)
                if arxiv_id:
                    pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"

            arxiv_id = _extract_arxiv_id(landing_page or "")
            source_id = f"arxiv:{arxiv_id}" if arxiv_id else _stable_id("arxiv", title, year)

            records.append(
                SourcePaperRecord(
                    source="arxiv",
                    source_id=source_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=published,
                    venue="arXiv",
                    doi=doi,
                    authors=authors,
                    abstract=abstract,
                    landing_page_url=landing_page,
                    pdf_url=pdf_url,
                    relevance_score=0.84,
                    raw=(None if not config.include_raw_payloads else {"id": landing_page, "doi": doi}),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        if len(entries) < batch_size:
            break
        start += batch_size

    return records, None


def collect_google_scholar_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    if config.serpapi_api_key:
        return _collect_google_scholar_via_serpapi(member, client, config, min_year)

    records: list[SourcePaperRecord] = []
    query = f'author:"{member.name}"'

    for page in range(config.max_google_scholar_pages):
        start = page * 10
        params = {
            "q": query,
            "hl": "en",
            "as_ylo": str(min_year),
            "start": str(start),
        }
        url = f"{config.google_scholar_search_url}?{urllib.parse.urlencode(params)}"
        html, error = client.fetch_text(url, headers={"Accept": "text/html"}, prefer="expedition")
        if error or not html:
            if records:
                return records, None
            return [], error or "No Google Scholar response"

        lower_html = html.lower()
        if "unusual traffic" in lower_html or "prove you're not a robot" in lower_html:
            if records:
                return records, None
            return [], "Google Scholar blocked the request (captcha/unusual traffic)"

        soup = BeautifulSoup(html, "lxml")
        rows = soup.select("div.gs_ri")
        if not rows:
            break

        for row in rows:
            title_node = row.select_one("h3.gs_rt")
            if title_node is None:
                continue
            link_node = title_node.select_one("a")
            title = _clean_text(title_node.get_text(" ", strip=True))
            title = re.sub(r"^\[[^\]]+\]\s*", "", title)
            if not title:
                continue

            meta_text = _clean_text((row.select_one("div.gs_a") or row).get_text(" ", strip=True))
            year = _year_from_text(meta_text)
            if year is not None and year < min_year:
                continue

            snippet = _clean_optional(_clean_text((row.select_one("div.gs_rs") or row).get_text(" ", strip=True)))
            landing_page = _clean_optional(link_node.get("href") if link_node else None)

            authors = _scholar_authors(meta_text)
            if authors and not any(_author_matches_member(member.name, author) for author in authors):
                continue

            venue = _scholar_venue(meta_text)
            source_id = _stable_id("scholar", title, year)

            records.append(
                SourcePaperRecord(
                    source="google_scholar",
                    source_id=source_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=_year_to_iso(year),
                    venue=venue,
                    doi=_normalize_doi(_extract_doi_from_text(meta_text + " " + (landing_page or ""))),
                    authors=authors,
                    abstract=snippet,
                    landing_page_url=landing_page,
                    pdf_url=_pick_pdf_url([landing_page] if landing_page else []),
                    relevance_score=0.7,
                    raw=(None if not config.include_raw_payloads else {"meta": meta_text}),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        next_button = soup.select_one("td a[aria-label='Next']")
        if next_button is None:
            break

    return records, None


def _collect_google_scholar_via_serpapi(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    records: list[SourcePaperRecord] = []
    query = f'author:"{member.name}"'

    for page in range(config.max_google_scholar_pages):
        start = page * 10
        params = {
            "engine": "google_scholar",
            "q": query,
            "hl": "en",
            "as_ylo": str(min_year),
            "start": str(start),
            "api_key": config.serpapi_api_key or "",
        }
        url = f"{config.google_scholar_serpapi_api}?{urllib.parse.urlencode(params)}"
        payload, error = client.fetch_json(url, headers={"Accept": "application/json"}, prefer="requests")
        if error or not isinstance(payload, dict):
            if records:
                return records, None
            return [], error or "No SerpAPI response"

        if payload.get("error"):
            message = _clean_text(str(payload.get("error", ""))) or "SerpAPI error"
            if records:
                return records, None
            return [], message

        rows = payload.get("organic_results")
        if not isinstance(rows, list) or not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue
            title = _clean_text(str(row.get("title", "")))
            if not title:
                continue

            publication_info = row.get("publication_info") if isinstance(row.get("publication_info"), dict) else {}
            summary = _clean_text(str(publication_info.get("summary", "")))
            year = _year_from_text(summary)
            if year is not None and year < min_year:
                continue

            authors = _serpapi_authors(publication_info.get("authors"))
            if authors and not any(_author_matches_member(member.name, author) for author in authors):
                continue

            snippet = _clean_optional(_clean_text(str(row.get("snippet", ""))))
            landing_page = _clean_optional(str(row.get("link", "")))
            resources = row.get("resources") if isinstance(row.get("resources"), list) else []
            pdf_url = None
            for resource in resources:
                if not isinstance(resource, dict):
                    continue
                candidate = _clean_optional(str(resource.get("link", "")))
                if candidate and candidate.lower().endswith(".pdf"):
                    pdf_url = candidate
                    break
            if pdf_url is None and landing_page:
                pdf_url = _pick_pdf_url([landing_page])

            source_id = _stable_id("scholar", title, year)
            records.append(
                SourcePaperRecord(
                    source="google_scholar",
                    source_id=source_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=_year_to_iso(year),
                    venue=_scholar_venue(summary),
                    doi=_normalize_doi(_extract_doi_from_text(summary + " " + (landing_page or ""))),
                    authors=authors,
                    abstract=snippet,
                    landing_page_url=landing_page,
                    pdf_url=pdf_url,
                    relevance_score=0.74,
                    raw=(row if config.include_raw_payloads else None),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        if len(rows) < 10:
            break

    return records, None


def _score_author_name(target: str, candidate: str) -> float:
    target_tokens = _tokens(target)
    candidate_tokens = _tokens(candidate)
    if not target_tokens or not candidate_tokens:
        return -100.0

    score = 0.0
    if target_tokens[-1] == candidate_tokens[-1]:
        score += 30.0
    elif target_tokens[-1] in candidate_tokens:
        score += 10.0
    else:
        score -= 30.0

    if target_tokens[0] == candidate_tokens[0]:
        score += 30.0
    elif target_tokens[0][0] == candidate_tokens[0][0]:
        score += 12.0
    else:
        score -= 12.0

    score += len(set(target_tokens).intersection(candidate_tokens)) * 6.0
    return score


def _tokens(value: str) -> list[str]:
    cleaned = re.sub(r"[^A-Za-z ]", " ", value or "")
    return [part.lower() for part in cleaned.split() if part]


def _author_matches_member(member_name: str, author_name: str) -> bool:
    member_tokens = _tokens(member_name)
    author_tokens = _tokens(author_name)
    if len(member_tokens) < 2 or len(author_tokens) < 2:
        return False

    member_last = member_tokens[-1]
    author_last = author_tokens[-1]
    if member_last != author_last:
        return False

    member_first = member_tokens[0]
    author_first = author_tokens[0]
    return member_first == author_first or member_first[0] == author_first[0]


def _safe_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _crossref_authors(raw_authors: list[dict]) -> list[str]:
    out: list[str] = []
    for raw in raw_authors:
        if not isinstance(raw, dict):
            continue
        given = _clean_text(str(raw.get("given", "")))
        family = _clean_text(str(raw.get("family", "")))
        literal = _clean_text(str(raw.get("name", "")))
        if given or family:
            out.append(" ".join(part for part in [given, family] if part))
        elif literal:
            out.append(literal)
    return [name for name in out if name]


def _crossref_year(item: dict) -> int | None:
    for key in ["issued", "published-print", "published-online", "created"]:
        value = item.get(key)
        if not isinstance(value, dict):
            continue
        parts = value.get("date-parts")
        if not isinstance(parts, list) or not parts:
            continue
        first = parts[0]
        if isinstance(first, list) and first:
            year = _safe_int(first[0])
            if year:
                return year
    return None


def _crossref_date(item: dict) -> str | None:
    for key in ["published-online", "published-print", "issued"]:
        value = item.get(key)
        if not isinstance(value, dict):
            continue
        parts = value.get("date-parts")
        if not isinstance(parts, list) or not parts:
            continue
        first = parts[0]
        if not isinstance(first, list) or not first:
            continue
        year = str(first[0])
        month = f"{int(first[1]):02d}" if len(first) > 1 else "01"
        day = f"{int(first[2]):02d}" if len(first) > 2 else "01"
        return f"{year}-{month}-{day}"
    return None


def _crossref_container(item: dict) -> str | None:
    candidates = item.get("container-title")
    if isinstance(candidates, list) and candidates:
        return _clean_optional(str(candidates[0]))
    return None


def _crossref_pdf(raw_links: list[dict]) -> str | None:
    for raw in raw_links:
        if not isinstance(raw, dict):
            continue
        content_type = _clean_optional(str(raw.get("content-type", ""))) or ""
        url = _clean_optional(str(raw.get("URL", "")))
        if url and "pdf" in content_type.lower():
            return url
    return None


def _scholar_authors(meta: str) -> list[str]:
    if not meta:
        return []
    left = meta.split(" - ", 1)[0]
    values = [_clean_text(part) for part in left.split(",")]
    return [value for value in values if value]


def _serpapi_authors(raw_authors: object) -> list[str]:
    if not isinstance(raw_authors, list):
        return []
    out: list[str] = []
    for item in raw_authors:
        if not isinstance(item, dict):
            continue
        name = _clean_text(str(item.get("name", "")))
        if name:
            out.append(name)
    return out


def _scholar_venue(meta: str) -> str | None:
    if " - " not in meta:
        return None
    parts = meta.split(" - ")
    if len(parts) < 2:
        return None
    return _clean_optional(_clean_text(parts[1]))


def _pick_pdf_url(urls: list[str]) -> str | None:
    for url in urls:
        if not url:
            continue
        stripped = url.strip()
        if stripped.lower().endswith(".pdf"):
            return stripped
        arxiv_id = _extract_arxiv_id(stripped)
        if arxiv_id:
            return f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    return None


def _extract_arxiv_id(value: str) -> str | None:
    match = ARXIV_ID_RE.search(value or "")
    if match:
        return match.group(1)
    return None


def _extract_doi_from_text(value: str) -> str | None:
    if not value:
        return None
    doi_match = re.search(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", value, re.IGNORECASE)
    if doi_match:
        return doi_match.group(0)
    return None


def _normalize_doi(value: str | None) -> str | None:
    text = _clean_optional(value)
    if not text:
        return None
    lowered = text.lower()
    if "doi.org/" in lowered:
        text = text.split("doi.org/", 1)[1]
    text = text.strip().strip(".")
    return text or None


def _stable_id(prefix: str, title: str, year: int | None) -> str:
    seed = f"{prefix}|{year or 'na'}|{title.lower()}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}:{digest}"


def _clean_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.split())
    return cleaned or None


def _clean_text(value: str) -> str:
    return " ".join((value or "").split())


def _strip_tags(value: str | None) -> str | None:
    text = _clean_optional(value)
    if not text:
        return None
    return _clean_optional(TAG_RE.sub(" ", text))


def _xml_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return "".join(node.itertext())


def _year_from_text(value: str | None) -> int | None:
    if not value:
        return None
    match = YEAR_RE.search(value)
    if not match:
        return None
    return _safe_int(match.group(0))


def _year_to_iso(year: int | None) -> str | None:
    if year is None:
        return None
    return f"{year}-01-01"
