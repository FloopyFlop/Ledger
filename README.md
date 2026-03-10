# Ledger

Ledger is a proxy-only, stateless paper collector for AI-MI faculty.

Each run starts from scratch, scrapes the AI-MI team page, queries multiple scholarly sources, and writes a standardized canonical dataset locally.

## What it collects

Enabled by default:

1. DBLP
2. OpenAlex
3. Semantic Scholar
4. Crossref
5. DataCite
6. Europe PMC
7. PubMed
8. OpenAIRE
9. DOAJ
10. arXiv
11. InspireHEP
12. Google Scholar

For each source hit, Ledger stores normalized fields (title, year, DOI, authors, URLs, matched AIMI member, and source metadata), then deduplicates into canonical paper records.
It also enriches `pdf_urls` from DOI/URL/source hints (including arXiv IDs like `10.48550/arXiv.2601.07742`).

This gives broad journal/preprint coverage via major bibliographic indexes rather than scraping individual publisher sites one-by-one.

## Proxy policy

Ledger enforces proxy-only networking. If no proxy is configured, it fails fast.

Set proxy credentials in `.env`:

```bash
LEDGER_PROXY_URL=http://user:pass@host:port
```

## Quick start

```bash
cd /Users/abm/XVOL/Cornell/AIMI/Ledger
cp .env.example .env
uv sync --extra dev
uv run ledger
```

Optional overrides:

```bash
uv run ledger --env-file .env --lookback-years 3 --member-limit 10
```

## Proxy compatibility

- Some providers force HTTPS endpoints and may fail with specific proxy exits.
- Source toggles in `.env` let you run only compatible sources.
- New broad sources (`OpenAIRE`, `DOAJ`) are page-capped by default (`LEDGER_MAX_OPENAIRE_PAGES`, `LEDGER_MAX_DOAJ_PAGES`) to keep runtime bounded.
- The included `.env` is configured for reliable operation with the current proxy (`OpenAlex` enabled by default).
- Re-enable other sources by setting the `LEDGER_ENABLE_*` flags to `true` (including `DATACITE`, `EUROPE_PMC`, `PUBMED`, `INSPIREHEP`, etc.).
- Ledger probes each enabled source via proxy before collection and auto-skips unreachable sources (`LEDGER_PROBE_SOURCES_BEFORE_COLLECTION=true`).
- arXiv is normalized to `http://export.arxiv.org/api/query` for better compatibility with rotating proxies.
- For Google Scholar, set `LEDGER_SERPAPI_API_KEY` to use SerpAPI instead of direct scraping when Scholar blocks requests.

## Output layout

Each run writes to `output/runs/<timestamp>/`:

- `members.json`
- `sources/dblp.json`
- `sources/openalex.json`
- `sources/semantic_scholar.json`
- `sources/crossref.json`
- `sources/datacite.json`
- `sources/europe_pmc.json`
- `sources/pubmed.json`
- `sources/openaire.json`
- `sources/doaj.json`
- `sources/arxiv.json`
- `sources/inspirehep.json`
- `sources/google_scholar.json`
- `source_errors.json`
- `proxy_audit.json`
- `award_document_summary.json`
- `papers_canonical.json`
- `papers_with_award_mention.json`
- `papers_without_award_mention.json`
- `summary.json`
- `report.md`
- `target_doi_coverage.json` (when target DOIs are configured)

Latest snapshots are also copied to `output/latest/`.

`award_document_summary.json` is compact and Slack-friendly: title/DOI/member/source/PDF URL + local PDF/PDF-A paths and conversion status (no abstract payloads).

## Standardized schema

Canonical papers (`papers_canonical.json`) include:

- `canonical_id`
- `title`
- `year`
- `published_date`
- `venue`
- `doi`
- `authors`
- `aimi_members`
- `abstract`
- `urls`
- `pdf_urls`
- `sources`
- `source_records`
- `award_mentioned_in_metadata`
- `award_mentions`
- `award_mentioned_in_document`
- `document_award_mentions`
- `document_award_context`
- `document_pdf_url`
- `document_pdf_local_path`
- `document_pdfa_path`
- `document_pdfa_error`
- `document_scan_error`

## Notes

- Google Scholar can rate-limit or challenge traffic; Ledger records source errors and continues.
- Award matching checks metadata and can also scan downloaded PDFs (`LEDGER_SCAN_PDFS_FOR_AWARDS=true`).
- When document scanning is enabled, Ledger can convert award-containing PDFs to PDF/A (`LEDGER_CONVERT_AWARD_PDFS_TO_PDFA=true`) and records outcomes in `award_document_summary.json`.
- PDF/A conversion uses Ghostscript (`LEDGER_GHOSTSCRIPT_BIN`, default `gs`); if unavailable, conversion errors are captured per document in the summary JSON.
- Default award regexes always include number-only matching for `2433348`.
- Some publisher PDF endpoints return `HTTP 403` through proxy-only routing; those are captured in `document_scan_error`.
- You can enforce expected-paper coverage by setting `LEDGER_TARGET_DOIS` or `LEDGER_TARGET_DOI_FILE` and enabling `LEDGER_FAIL_ON_MISSING_TARGET_DOIS=true`.
