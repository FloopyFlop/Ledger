from __future__ import annotations

import hashlib
import re
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
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


def collect_datacite_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    page_size = min(100, config.max_results_per_member_per_source)
    page_number = 1
    records: list[SourcePaperRecord] = []

    while len(records) < config.max_results_per_member_per_source:
        params = {
            "query": f'creators.name:"{member.name}"',
            "page[size]": str(page_size),
            "page[number]": str(page_number),
        }
        url = f"{config.datacite_works_api}?{urllib.parse.urlencode(params)}"
        payload, error = client.fetch_json(
            url,
            headers={"Accept": "application/vnd.api+json"},
            prefer="requests",
        )
        if error:
            if records:
                return records, None
            return [], error
        if not isinstance(payload, dict):
            break

        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue
            attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
            title = _datacite_title(attrs)
            if not title:
                continue

            published_date = _clean_optional(str(attrs.get("published", "")))
            year = _safe_int(attrs.get("publicationYear")) or _year_from_text(published_date)
            if year is not None and year < min_year:
                continue

            authors = _datacite_creators(attrs.get("creators"))
            if authors and not any(_author_matches_member(member.name, author) for author in authors):
                continue

            doi = _normalize_doi(str(attrs.get("doi", "")))
            source_id = doi or _clean_optional(str(row.get("id", ""))) or _stable_id("datacite", title, year)
            landing_page = _clean_optional(str(attrs.get("url", "")))
            if not landing_page and doi:
                landing_page = f"https://doi.org/{doi}"

            records.append(
                SourcePaperRecord(
                    source="datacite",
                    source_id=source_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=published_date or _year_to_iso(year),
                    venue=_clean_optional(str(attrs.get("publisher", ""))),
                    doi=doi,
                    authors=authors,
                    abstract=_datacite_abstract(attrs),
                    landing_page_url=landing_page,
                    pdf_url=_datacite_pdf_url(attrs),
                    relevance_score=0.82,
                    raw=(row if config.include_raw_payloads else None),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        if len(rows) < page_size:
            break
        page_number += 1

    return records, None


def collect_europe_pmc_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    page_size = min(100, config.max_results_per_member_per_source)
    page = 1
    records: list[SourcePaperRecord] = []

    query = f'AUTH:"{member.name}" AND FIRST_PDATE:[{min_year}-01-01 TO 3000-12-31]'
    while len(records) < config.max_results_per_member_per_source:
        params = {
            "query": query,
            "format": "json",
            "pageSize": str(page_size),
            "page": str(page),
        }
        url = f"{config.europe_pmc_search_api}?{urllib.parse.urlencode(params)}"
        payload, error = client.fetch_json(url, headers={"Accept": "application/json"}, prefer="requests")
        if error:
            if records:
                return records, None
            return [], error
        if not isinstance(payload, dict):
            break

        result_list = payload.get("resultList") if isinstance(payload.get("resultList"), dict) else {}
        rows = result_list.get("result") if isinstance(result_list, dict) else []
        if not isinstance(rows, list) or not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue
            title = _clean_text(str(row.get("title", "")))
            if not title:
                continue

            published_date = (
                _clean_optional(str(row.get("firstPublicationDate", "")))
                or _clean_optional(str(row.get("electronicPublicationDate", "")))
            )
            year = _safe_int(row.get("pubYear")) or _year_from_text(published_date)
            if year is not None and year < min_year:
                continue

            authors = _europe_pmc_authors(row)
            if authors and not any(_author_matches_member(member.name, author) for author in authors):
                continue

            doi = _normalize_doi(str(row.get("doi", "")))
            pmid = _clean_optional(str(row.get("pmid", "")))
            pmcid = _clean_optional(str(row.get("pmcid", "")))
            source = _clean_optional(str(row.get("source", "")))
            native_id = _clean_optional(str(row.get("id", "")))

            landing_page = None
            if doi:
                landing_page = f"https://doi.org/{doi}"
            elif pmcid:
                landing_page = f"https://pmc.ncbi.nlm.nih.gov/articles/{_normalize_pmcid(pmcid)}/"
            elif pmid:
                landing_page = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
            elif source and native_id:
                landing_page = f"https://europepmc.org/article/{source}/{native_id}"

            source_id = (
                doi
                or (f"pmid:{pmid}" if pmid else None)
                or (f"pmcid:{pmcid}" if pmcid else None)
                or (f"europepmc:{source}:{native_id}" if source and native_id else None)
                or _stable_id("europe_pmc", title, year)
            )

            records.append(
                SourcePaperRecord(
                    source="europe_pmc",
                    source_id=source_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=published_date or _year_to_iso(year),
                    venue=_clean_optional(str(row.get("journalTitle", ""))) or "Europe PMC",
                    doi=doi,
                    authors=authors,
                    abstract=_clean_optional(_clean_text(str(row.get("abstractText", "")))),
                    landing_page_url=landing_page,
                    pdf_url=_europe_pmc_pdf_url(row),
                    relevance_score=0.8,
                    raw=(row if config.include_raw_payloads else None),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        if len(rows) < page_size:
            break
        page += 1

    return records, None


def collect_pubmed_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    retmax = min(100, config.max_results_per_member_per_source)
    retstart = 0
    records: list[SourcePaperRecord] = []

    while len(records) < config.max_results_per_member_per_source:
        search_params = {
            "db": "pubmed",
            "retmode": "json",
            "retmax": str(retmax),
            "retstart": str(retstart),
            "sort": "pub date",
            "term": _pubmed_author_query(member.name, min_year=min_year),
            "tool": config.pubmed_tool,
        }
        if config.pubmed_email:
            search_params["email"] = config.pubmed_email
        search_url = f"{config.pubmed_esearch_api}?{urllib.parse.urlencode(search_params)}"
        search_payload, search_error = client.fetch_json(
            search_url,
            headers={"Accept": "application/json"},
            prefer="requests",
        )
        if search_error:
            if records:
                return records, None
            return [], search_error
        if not isinstance(search_payload, dict):
            break

        pmids = _pubmed_id_list(search_payload)
        if not pmids:
            break

        fetch_params = {
            "db": "pubmed",
            "retmode": "xml",
            "id": ",".join(pmids),
            "tool": config.pubmed_tool,
        }
        if config.pubmed_email:
            fetch_params["email"] = config.pubmed_email
        fetch_url = f"{config.pubmed_efetch_api}?{urllib.parse.urlencode(fetch_params)}"
        xml_text, fetch_error = client.fetch_text(
            fetch_url,
            headers={"Accept": "application/xml,text/xml"},
            prefer="requests",
        )
        if fetch_error:
            if records:
                return records, None
            return [], fetch_error
        if not xml_text:
            break

        rows, parse_error = _parse_pubmed_fetch_xml(xml_text)
        if parse_error:
            if records:
                return records, None
            return [], parse_error
        if not rows:
            break

        for row in rows:
            title = row.get("title")
            if not isinstance(title, str) or not title:
                continue

            year = _safe_int(row.get("year"))
            if year is not None and year < min_year:
                continue

            authors = row.get("authors")
            if not isinstance(authors, list):
                authors = []
            if authors and not any(_pubmed_author_matches_member(member.name, author) for author in authors):
                continue

            pmid = _clean_optional(str(row.get("pmid", "")))
            if not pmid:
                continue
            doi = _normalize_doi(str(row.get("doi", "")))
            pmcid = _normalize_pmcid(_clean_optional(str(row.get("pmcid", ""))))
            landing_page = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
            if doi:
                landing_page = f"https://doi.org/{doi}"

            records.append(
                SourcePaperRecord(
                    source="pubmed",
                    source_id=(doi or f"pmid:{pmid}"),
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=_clean_optional(str(row.get("published_date", ""))) or _year_to_iso(year),
                    venue=_clean_optional(str(row.get("journal", ""))) or "PubMed",
                    doi=doi,
                    authors=authors,
                    abstract=_clean_optional(str(row.get("abstract", ""))),
                    landing_page_url=landing_page,
                    pdf_url=(f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/" if pmcid else None),
                    relevance_score=0.83,
                    raw=(row if config.include_raw_payloads else None),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        if len(pmids) < retmax:
            break
        retstart += retmax

    return records, None


def collect_openaire_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    page_size = min(100, config.max_results_per_member_per_source)
    page = 1
    records: list[SourcePaperRecord] = []

    while len(records) < config.max_results_per_member_per_source and page <= config.max_openaire_pages:
        params = {
            "keywords": f'"{member.name}"',
            "size": str(page_size),
            "page": str(page),
        }
        url = f"{config.openaire_publications_api}?{urllib.parse.urlencode(params)}"
        xml_text, error = client.fetch_text(url, headers={"Accept": "application/xml,text/xml"}, prefer="requests")
        if error or not xml_text:
            if records:
                return records, None
            return [], error or "No OpenAIRE response"

        rows, parse_error, total_pages = _parse_openaire_results(xml_text)
        if parse_error:
            if records:
                return records, None
            return [], parse_error
        if not rows:
            break

        for row in rows:
            title = _clean_optional(str(row.get("title", "")))
            if not title:
                continue

            year = _safe_int(row.get("year")) or _year_from_text(_clean_optional(str(row.get("published_date", ""))))
            if year is not None and year < min_year:
                continue

            authors = row.get("authors")
            if not isinstance(authors, list):
                authors = []
            if authors and not any(_author_matches_member(member.name, author) for author in authors):
                continue

            doi = _normalize_doi(_clean_optional(str(row.get("doi", ""))))
            urls = row.get("urls")
            if not isinstance(urls, list):
                urls = []
            landing_page = urls[0] if urls else None
            pdf_url = _pick_pdf_url(urls)
            source_id = doi or _stable_id("openaire", title, year)

            records.append(
                SourcePaperRecord(
                    source="openaire",
                    source_id=source_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=_clean_optional(str(row.get("published_date", ""))) or _year_to_iso(year),
                    venue=_clean_optional(str(row.get("venue", ""))) or "OpenAIRE",
                    doi=doi,
                    authors=authors,
                    abstract=_clean_optional(str(row.get("abstract", ""))),
                    landing_page_url=landing_page,
                    pdf_url=pdf_url,
                    relevance_score=0.78,
                    raw=(row if config.include_raw_payloads else None),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        if len(rows) < page_size:
            break
        if total_pages is not None and page >= total_pages:
            break
        page += 1

    return records, None


def collect_doaj_for_member(
    member: Member,
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], str | None]:
    page_size = min(100, config.max_results_per_member_per_source)
    page = 1
    records: list[SourcePaperRecord] = []

    while len(records) < config.max_results_per_member_per_source and page <= config.max_doaj_pages:
        query = urllib.parse.quote(member.name)
        url = f"{config.doaj_articles_api}/{query}?page={page}&pageSize={page_size}"
        payload, error = client.fetch_json(url, headers={"Accept": "application/json"}, prefer="requests")
        if error:
            if records:
                return records, None
            return [], error
        if not isinstance(payload, dict):
            break

        rows = payload.get("results")
        if not isinstance(rows, list) or not rows:
            break

        for row in rows:
            if not isinstance(row, dict):
                continue
            bib = row.get("bibjson") if isinstance(row.get("bibjson"), dict) else {}
            title = _clean_optional(str(bib.get("title", "")))
            if not title:
                continue

            year = _safe_int(bib.get("year")) or _year_from_text(_clean_optional(str(bib.get("last_updated", ""))))
            if year is not None and year < min_year:
                continue

            authors = _doaj_authors(bib.get("author"))
            if authors and not any(_author_matches_member(member.name, author) for author in authors):
                continue

            doi = _doaj_doi(bib.get("identifier"))
            links = _doaj_links(bib.get("link"))
            source_id = doi or _clean_optional(str(row.get("id", ""))) or _stable_id("doaj", title, year)
            venue = _doaj_journal_title(bib)
            landing_page = links[0] if links else None

            records.append(
                SourcePaperRecord(
                    source="doaj",
                    source_id=source_id,
                    member_name=member.name,
                    title=title,
                    year=year,
                    published_date=_year_to_iso(year),
                    venue=venue or "DOAJ",
                    doi=doi,
                    authors=authors,
                    abstract=_clean_optional(str(bib.get("abstract", ""))),
                    landing_page_url=landing_page,
                    pdf_url=_pick_pdf_url(links),
                    relevance_score=0.74,
                    raw=(row if config.include_raw_payloads else None),
                )
            )
            if len(records) >= config.max_results_per_member_per_source:
                return records, None

        if len(rows) < page_size:
            break
        page += 1

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
    query = _arxiv_author_query(member.name)

    while start < config.max_results_per_member_per_source:
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


def collect_inspirehep_for_members(
    *,
    members: list[Member],
    client: HttpClient,
    config: LedgerConfig,
    min_year: int,
) -> tuple[list[SourcePaperRecord], list[dict[str, str]]]:
    affiliation_id = _clean_optional(config.inspirehep_affiliation_id)
    if not affiliation_id:
        return [], [{"member": "<all>", "error": "InspireHEP affiliation id is not configured"}]

    max_year = datetime.now(tz=timezone.utc).year
    query = f"affid:{affiliation_id} and date {min_year}->{max_year}"
    page_size = max(1, min(config.inspirehep_page_size, 250))
    max_records = max(1, config.inspirehep_max_records)
    params = {
        "q": query,
        "size": str(page_size),
        "sort": "mostrecent",
    }
    next_url: str | None = f"{config.inspirehep_literature_api}?{urllib.parse.urlencode(params)}"

    records: list[SourcePaperRecord] = []
    errors: list[dict[str, str]] = []
    processed = 0

    while next_url and processed < max_records:
        payload, error = client.fetch_json(next_url, headers={"Accept": "application/json"}, prefer="requests")
        if error or not isinstance(payload, dict):
            if records:
                errors.append({"member": "<all>", "error": error or "Invalid InspireHEP payload"})
                return records, errors
            return [], [{"member": "<all>", "error": error or "Invalid InspireHEP payload"}]

        hits = payload.get("hits") if isinstance(payload.get("hits"), dict) else {}
        rows = hits.get("hits") if isinstance(hits, dict) else []
        if not isinstance(rows, list) or not rows:
            break

        for row in rows:
            if processed >= max_records:
                break
            processed += 1
            if not isinstance(row, dict):
                continue

            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            if not metadata:
                continue

            title = _inspire_title(metadata)
            if not title:
                continue

            published_date = _inspire_published_date(metadata)
            year = _year_from_text(published_date)
            if year is None:
                year = _inspire_publication_year(metadata)
            if year is not None and year < min_year:
                continue

            authors = _inspire_authors(metadata)
            if not authors:
                continue

            matched_members = [
                member.name
                for member in members
                if any(_author_matches_member(member.name, author) for author in authors)
            ]
            if not matched_members:
                continue

            doi = _inspire_doi(metadata)
            abstract = _inspire_abstract(metadata)
            venue = _inspire_venue(metadata)
            landing_page = _inspire_landing_page(metadata)
            pdf_url = _inspire_pdf_url(metadata)
            source_id = doi or _inspire_source_id(metadata, title=title, year=year)

            for member_name in matched_members:
                records.append(
                    SourcePaperRecord(
                        source="inspirehep",
                        source_id=source_id,
                        member_name=member_name,
                        title=title,
                        year=year,
                        published_date=published_date,
                        venue=venue,
                        doi=doi,
                        authors=authors,
                        abstract=abstract,
                        landing_page_url=landing_page,
                        pdf_url=pdf_url,
                        relevance_score=0.88,
                        raw=(
                            None
                            if not config.include_raw_payloads
                            else {
                                "control_number": metadata.get("control_number"),
                                "arxiv_eprints": metadata.get("arxiv_eprints"),
                            }
                        ),
                    )
                )

        links = payload.get("links") if isinstance(payload.get("links"), dict) else {}
        next_candidate = links.get("next") if isinstance(links, dict) else None
        next_url = next_candidate if isinstance(next_candidate, str) and next_candidate.strip() else None

    return records, errors


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


def _arxiv_author_query(member_name: str) -> str:
    cleaned = _clean_text(member_name)
    if not cleaned:
        return 'au:""'

    tokens = _tokens(cleaned)
    variants = [cleaned]
    if len(tokens) >= 2:
        first = tokens[0]
        last = tokens[-1]
        variants.append(f"{first} {last}")
        variants.append(f"{first[0]} {last}")

    deduped: list[str] = []
    seen: set[str] = set()
    for value in variants:
        item = _clean_text(value)
        key = item.lower()
        if not item or key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    if len(deduped) == 1:
        return f'au:"{deduped[0]}"'
    return " OR ".join([f'au:"{value}"' for value in deduped])


def _tokens(value: str) -> list[str]:
    cleaned = re.sub(r"[^A-Za-z ]", " ", value or "")
    return [part.lower() for part in cleaned.split() if part]


def _datacite_title(attrs: dict) -> str:
    titles = attrs.get("titles") if isinstance(attrs.get("titles"), list) else []
    for item in titles:
        if not isinstance(item, dict):
            continue
        value = _clean_text(str(item.get("title", "")))
        if value:
            return value
    return ""


def _datacite_creators(raw_creators: object) -> list[str]:
    if not isinstance(raw_creators, list):
        return []
    names: list[str] = []
    for creator in raw_creators:
        if not isinstance(creator, dict):
            continue
        literal = _clean_optional(str(creator.get("name", "")))
        given = _clean_optional(str(creator.get("givenName", "")))
        family = _clean_optional(str(creator.get("familyName", "")))
        if literal:
            names.append(literal)
        elif given or family:
            names.append(_clean_text(" ".join(part for part in [given or "", family or ""] if part)))
    return _merge_unique(names)


def _datacite_abstract(attrs: dict) -> str | None:
    descriptions = attrs.get("descriptions") if isinstance(attrs.get("descriptions"), list) else []
    for item in descriptions:
        if not isinstance(item, dict):
            continue
        text = _clean_optional(str(item.get("description", "")))
        if text:
            return text
    return None


def _datacite_pdf_url(attrs: dict) -> str | None:
    content_urls = attrs.get("contentUrl") if isinstance(attrs.get("contentUrl"), list) else []
    for item in content_urls:
        candidate = _clean_optional(str(item))
        if not candidate:
            continue
        if candidate.lower().endswith(".pdf") or "/pdf" in candidate.lower():
            return candidate
    return None


def _europe_pmc_authors(row: dict) -> list[str]:
    raw = _clean_optional(str(row.get("authorString", "")))
    if not raw:
        return []
    values = [_clean_text(part) for part in re.split(r"[;,]", raw)]
    return [value for value in values if value]


def _europe_pmc_pdf_url(row: dict) -> str | None:
    full_texts = row.get("fullTextUrlList") if isinstance(row.get("fullTextUrlList"), dict) else {}
    links = full_texts.get("fullTextUrl") if isinstance(full_texts, dict) else []
    if isinstance(links, dict):
        links = [links]
    if isinstance(links, list):
        for link in links:
            if not isinstance(link, dict):
                continue
            url = _clean_optional(str(link.get("url", "")))
            style = _clean_optional(str(link.get("documentStyle", ""))) or ""
            if not url:
                continue
            if "pdf" in style.lower() or url.lower().endswith(".pdf") or "/pdf" in url.lower():
                return url

    pmcid = _normalize_pmcid(_clean_optional(str(row.get("pmcid", ""))))
    if pmcid:
        return f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/"
    return None


def _normalize_pmcid(value: str | None) -> str | None:
    text = _clean_optional(value)
    if not text:
        return None
    if text.upper().startswith("PMC"):
        return "PMC" + text[3:]
    digits = re.sub(r"\D+", "", text)
    if digits:
        return f"PMC{digits}"
    return text


def _pubmed_author_query(member_name: str, *, min_year: int) -> str:
    tokens = _tokens(member_name)
    if len(tokens) < 2:
        return f'"{member_name}"[Author]'
    last = tokens[-1]
    first = tokens[0]
    initial = first[0]
    author_variants = [
        f'"{last} {initial}"[Author]',
        f'"{member_name}"[Author]',
    ]
    author_clause = " OR ".join(author_variants)
    date_clause = f'("{min_year}/01/01"[Date - Publication] : "3000"[Date - Publication])'
    return f"({author_clause}) AND {date_clause}"


def _pubmed_id_list(payload: dict) -> list[str]:
    result = payload.get("esearchresult") if isinstance(payload.get("esearchresult"), dict) else {}
    ids = result.get("idlist") if isinstance(result, dict) else []
    if not isinstance(ids, list):
        return []
    return [str(item).strip() for item in ids if str(item).strip()]


def _parse_pubmed_fetch_xml(xml_text: str) -> tuple[list[dict], str | None]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        return [], f"Invalid PubMed XML: {exc}"

    rows: list[dict] = []
    for article in root.findall(".//PubmedArticle"):
        citation = article.find("MedlineCitation")
        if citation is None:
            continue
        article_node = citation.find("Article")
        if article_node is None:
            continue

        pmid = _clean_optional(_xml_text(citation.find("PMID")))
        title = _clean_optional(_clean_text(_xml_text(article_node.find("ArticleTitle"))))
        if not pmid or not title:
            continue

        year, published_date = _pubmed_pubdate(citation, article_node)
        doi, pmcid = _pubmed_ids(article, article_node)
        authors = _pubmed_full_authors(article_node)
        abstract = _pubmed_abstract(article_node)
        journal = _clean_optional(_clean_text(_xml_text(article_node.find("Journal/Title"))))

        rows.append(
            {
                "pmid": pmid,
                "title": title,
                "year": year,
                "published_date": published_date,
                "doi": doi,
                "pmcid": pmcid,
                "authors": authors,
                "journal": journal,
                "abstract": abstract,
            }
        )

    return rows, None


def _pubmed_pubdate(citation: ET.Element, article_node: ET.Element) -> tuple[int | None, str | None]:
    candidates = [
        article_node.find("ArticleDate"),
        article_node.find("Journal/JournalIssue/PubDate"),
    ]
    for node in candidates:
        if node is None:
            continue
        year = _safe_int(_xml_text(node.find("Year")))
        month = _pubmed_month_to_num(_xml_text(node.find("Month")))
        day = _safe_int(_xml_text(node.find("Day"))) or 1
        if year is None:
            medline = _clean_optional(_xml_text(node.find("MedlineDate")))
            year = _year_from_text(medline)
            if year is None:
                continue
        return year, f"{year:04d}-{month:02d}-{day:02d}"

    completed_year = _safe_int(_xml_text(citation.find("DateCompleted/Year")))
    if completed_year is not None:
        return completed_year, f"{completed_year:04d}-01-01"
    return None, None


def _pubmed_month_to_num(value: str) -> int:
    text = _clean_optional(value)
    if not text:
        return 1
    as_int = _safe_int(text)
    if as_int is not None:
        return max(1, min(as_int, 12))
    key = text.strip()[:3].lower()
    mapping = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    return mapping.get(key, 1)


def _pubmed_ids(article: ET.Element, article_node: ET.Element) -> tuple[str | None, str | None]:
    doi = None
    pmcid = None

    for node in article.findall(".//ArticleId"):
        id_type = (node.attrib.get("IdType", "") or "").strip().lower()
        value = _clean_optional(_xml_text(node))
        if not value:
            continue
        if id_type == "doi" and doi is None:
            doi = _normalize_doi(value)
        elif id_type == "pmc" and pmcid is None:
            pmcid = _normalize_pmcid(value)

    if doi is None:
        for node in article_node.findall("ELocationID"):
            id_type = (node.attrib.get("EIdType", "") or "").strip().lower()
            value = _clean_optional(_xml_text(node))
            if id_type == "doi" and value:
                doi = _normalize_doi(value)
                break

    return doi, pmcid


def _pubmed_full_authors(article_node: ET.Element) -> list[str]:
    authors: list[str] = []
    for author in article_node.findall("AuthorList/Author"):
        collective = _clean_optional(_xml_text(author.find("CollectiveName")))
        if collective:
            authors.append(collective)
            continue

        last = _clean_optional(_xml_text(author.find("LastName")))
        fore = _clean_optional(_xml_text(author.find("ForeName")))
        initials = _clean_optional(_xml_text(author.find("Initials")))

        if fore and last:
            authors.append(_clean_text(f"{fore} {last}"))
        elif initials and last:
            authors.append(_clean_text(f"{initials} {last}"))
        elif last:
            authors.append(last)
    return _merge_unique(authors)


def _pubmed_abstract(article_node: ET.Element) -> str | None:
    parts: list[str] = []
    for node in article_node.findall("Abstract/AbstractText"):
        label = _clean_optional(node.attrib.get("Label"))
        text = _clean_optional(_clean_text(_xml_text(node)))
        if not text:
            continue
        if label:
            parts.append(f"{label}: {text}")
        else:
            parts.append(text)
    if not parts:
        return None
    return " ".join(parts)


def _pubmed_author_matches_member(member_name: str, author_name: str) -> bool:
    member_tokens = _tokens(member_name)
    author_tokens = _tokens(author_name)
    if len(member_tokens) < 2 or len(author_tokens) < 2:
        return False

    member_last = member_tokens[-1]
    member_given = member_tokens[:-1]

    if author_tokens[-1] == member_last:
        return _pubmed_given_name_match(member_given, author_tokens[:-1])
    if author_tokens[0] == member_last:
        return _pubmed_given_name_match(member_given, author_tokens[1:])
    return False


def _pubmed_given_name_match(member_given_parts: list[str], author_given_parts: list[str]) -> bool:
    if not member_given_parts or not author_given_parts:
        return False

    member_compact = "".join(member_given_parts)
    author_compact = "".join(author_given_parts)
    if not member_compact or not author_compact:
        return False

    if member_compact == author_compact:
        return True

    member_initials = "".join(part[0] for part in member_given_parts if part)
    author_initials = "".join(part[0] for part in author_given_parts if part)
    if author_compact == member_initials:
        return True
    if member_compact == author_initials and len(member_compact) <= 3:
        return True
    if len(author_compact) == 1 and author_compact == member_compact[0]:
        return True
    if len(member_compact) == 1 and member_compact == author_compact[0]:
        return True
    return False


def _parse_openaire_results(xml_text: str) -> tuple[list[dict], str | None, int | None]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        return [], f"Invalid OpenAIRE XML: {exc}", None

    total_pages = _safe_int(_first_local_text(root, "totalPages"))
    rows: list[dict] = []

    for result in _findall_local(root, "result"):
        title = _first_local_text(result, "title")
        if not title:
            continue

        creators = _all_local_text(result, "creator")
        date_values = (
            _all_local_text(result, "dateofacceptance")
            + _all_local_text(result, "publicationdate")
            + _all_local_text(result, "dateofcollection")
        )
        published_date = next((value for value in date_values if value), None)
        venue = _first_local_text(result, "journal") or _first_local_text(result, "publisher")

        urls = _merge_unique(_all_local_text(result, "url"))
        pids = _merge_unique(_all_local_text(result, "pid"))
        doi = _first_doi_from_values([*pids, *urls])
        year = _year_from_text(published_date)
        abstract = _first_local_text(result, "description")

        rows.append(
            {
                "title": title,
                "authors": _merge_unique(creators),
                "published_date": published_date,
                "year": year,
                "venue": venue,
                "doi": doi,
                "urls": urls,
                "abstract": abstract,
            }
        )

    return rows, None, total_pages


def _findall_local(node: ET.Element, local_name: str) -> list[ET.Element]:
    out: list[ET.Element] = []
    for child in node.iter():
        if child.tag.split("}")[-1] == local_name:
            out.append(child)
    return out


def _first_local_text(node: ET.Element, local_name: str) -> str | None:
    for child in _findall_local(node, local_name):
        text = _clean_optional(_xml_text(child))
        if text:
            return text
    return None


def _all_local_text(node: ET.Element, local_name: str) -> list[str]:
    out: list[str] = []
    for child in _findall_local(node, local_name):
        text = _clean_optional(_xml_text(child))
        if text:
            out.append(text)
    return out


def _first_doi_from_values(values: list[str]) -> str | None:
    for value in values:
        doi = _normalize_doi(value)
        if doi and doi.startswith("10."):
            return doi
        extracted = _extract_doi_from_text(value)
        normalized = _normalize_doi(extracted)
        if normalized:
            return normalized
    return None


def _doaj_authors(raw_authors: object) -> list[str]:
    if not isinstance(raw_authors, list):
        return []
    names: list[str] = []
    for item in raw_authors:
        if not isinstance(item, dict):
            continue
        name = _clean_optional(str(item.get("name", "")))
        if name:
            names.append(name)
    return _merge_unique(names)


def _doaj_doi(raw_identifiers: object) -> str | None:
    if not isinstance(raw_identifiers, list):
        return None
    for item in raw_identifiers:
        if not isinstance(item, dict):
            continue
        id_type = _clean_optional(str(item.get("type", "")))
        value = _clean_optional(str(item.get("id", "")))
        if not value:
            continue
        if id_type and id_type.lower() == "doi":
            return _normalize_doi(value)
    for item in raw_identifiers:
        if not isinstance(item, dict):
            continue
        value = _clean_optional(str(item.get("id", "")))
        normalized = _normalize_doi(value)
        if normalized:
            return normalized
    return None


def _doaj_links(raw_links: object) -> list[str]:
    if not isinstance(raw_links, list):
        return []
    values: list[str] = []
    for item in raw_links:
        if not isinstance(item, dict):
            continue
        url = _clean_optional(str(item.get("url", "")))
        if url:
            values.append(url)
    return _merge_unique(values)


def _doaj_journal_title(bibjson: dict) -> str | None:
    journal = bibjson.get("journal") if isinstance(bibjson.get("journal"), dict) else {}
    return _clean_optional(str(journal.get("title", ""))) if isinstance(journal, dict) else None


def _inspire_title(metadata: dict) -> str:
    titles = metadata.get("titles") if isinstance(metadata.get("titles"), list) else []
    if not titles:
        return ""
    first = titles[0] if isinstance(titles[0], dict) else {}
    return _clean_text(str((first or {}).get("title", "")))


def _inspire_published_date(metadata: dict) -> str | None:
    for key in ("preprint_date", "earliest_date", "legacy_creation_date"):
        value = _clean_optional(str(metadata.get(key, "")))
        if value:
            return value
    year = _inspire_publication_year(metadata)
    if year:
        return _year_to_iso(year)
    return None


def _inspire_publication_year(metadata: dict) -> int | None:
    publication_info = metadata.get("publication_info") if isinstance(metadata.get("publication_info"), list) else []
    if not publication_info:
        return None
    first = publication_info[0] if isinstance(publication_info[0], dict) else {}
    return _safe_int((first or {}).get("year"))


def _inspire_venue(metadata: dict) -> str | None:
    publication_info = metadata.get("publication_info") if isinstance(metadata.get("publication_info"), list) else []
    if publication_info and isinstance(publication_info[0], dict):
        first = publication_info[0]
        for key in ("journal_title", "conference_title", "pubinfo_freetext"):
            value = _clean_optional(str(first.get(key, "")))
            if value:
                return value
    return "InspireHEP"


def _inspire_abstract(metadata: dict) -> str | None:
    abstracts = metadata.get("abstracts") if isinstance(metadata.get("abstracts"), list) else []
    if not abstracts:
        return None
    first = abstracts[0] if isinstance(abstracts[0], dict) else {}
    return _clean_optional(_clean_text(str((first or {}).get("value", ""))))


def _inspire_doi(metadata: dict) -> str | None:
    dois = metadata.get("dois") if isinstance(metadata.get("dois"), list) else []
    if not dois:
        return None
    first = dois[0] if isinstance(dois[0], dict) else {}
    return _normalize_doi(str((first or {}).get("value", "")))


def _inspire_landing_page(metadata: dict) -> str | None:
    control_number = _safe_int(metadata.get("control_number"))
    if control_number:
        return f"https://inspirehep.net/literature/{control_number}"
    return None


def _inspire_pdf_url(metadata: dict) -> str | None:
    documents = metadata.get("documents") if isinstance(metadata.get("documents"), list) else []
    for doc in documents:
        if not isinstance(doc, dict):
            continue
        candidate = _clean_optional(str(doc.get("url", "")))
        if candidate and (candidate.lower().endswith(".pdf") or "/pdf" in candidate.lower()):
            return candidate

    arxiv = metadata.get("arxiv_eprints") if isinstance(metadata.get("arxiv_eprints"), list) else []
    if arxiv and isinstance(arxiv[0], dict):
        arxiv_id = _clean_optional(str(arxiv[0].get("value", "")))
        if arxiv_id:
            return f"https://arxiv.org/pdf/{arxiv_id}.pdf"

    urls = metadata.get("urls") if isinstance(metadata.get("urls"), list) else []
    for item in urls:
        if not isinstance(item, dict):
            continue
        candidate = _clean_optional(str(item.get("value", "")))
        if candidate and (candidate.lower().endswith(".pdf") or "/pdf" in candidate.lower()):
            return candidate
    return None


def _inspire_authors(metadata: dict) -> list[str]:
    raw = metadata.get("authors") if isinstance(metadata.get("authors"), list) else []
    out: list[str] = []
    for author in raw:
        if not isinstance(author, dict):
            continue
        name = (
            _clean_optional(str(author.get("full_name", "")))
            or _clean_optional(str(author.get("full_name_unicode_normalized", "")))
        )
        if not name:
            continue
        out.append(_inspire_normalize_author(name))
    return _merge_unique(out)


def _inspire_normalize_author(value: str) -> str:
    clean = _clean_text(value)
    if "," not in clean:
        return clean
    left, right = clean.split(",", 1)
    return _clean_text(f"{right} {left}")


def _inspire_source_id(metadata: dict, *, title: str, year: int | None) -> str:
    control_number = _safe_int(metadata.get("control_number"))
    if control_number:
        return f"inspirehep:{control_number}"
    return _stable_id("inspirehep", title, year)


def _merge_unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _clean_text(value)
        key = cleaned.lower()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
    return out


def _author_matches_member(member_name: str, author_name: str) -> bool:
    member_tokens = _tokens(member_name)
    author_tokens = _tokens(author_name)
    if len(member_tokens) < 2 or len(author_tokens) < 2:
        return False

    member_first = member_tokens[0]
    member_last = member_tokens[-1]
    author_first = author_tokens[0]
    author_last = author_tokens[-1]

    same_order = (
        author_last == member_last
        and (author_first == member_first or author_first[0] == member_first[0])
    )
    swapped_order = (
        author_first == member_last
        and (author_last == member_first or author_last[0] == member_first[0])
    )
    return same_order or swapped_order


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
