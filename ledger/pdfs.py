from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

from pypdf import PdfReader

from .models import Publication
from .net import HttpClient

ARXIV_ABS_RE = re.compile(r"arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)", re.IGNORECASE)
ARXIV_PDF_RE = re.compile(r"arxiv\.org/pdf/([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)\.pdf", re.IGNORECASE)
DOI_ARXIV_RE = re.compile(r"10\.48550/arXiv\.([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)", re.IGNORECASE)



def resolve_pdf_candidates(publication: Publication) -> list[str]:
    candidates: list[str] = []

    for link in publication.ee_urls:
        link = link.strip()
        if not link:
            continue

        if link.lower().endswith(".pdf"):
            candidates.append(link)

        arxiv_id = _extract_arxiv_id(link)
        if arxiv_id:
            candidates.append(f"https://arxiv.org/pdf/{arxiv_id}.pdf")

    if publication.doi:
        arxiv_id = _extract_arxiv_id(publication.doi)
        if arxiv_id:
            candidates.append(f"https://arxiv.org/pdf/{arxiv_id}.pdf")

    # Deduplicate while preserving order.
    unique: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        unique.append(candidate)
    return unique



def download_pdf(
    client: HttpClient,
    url: str,
    destination: Path,
    *,
    max_pdf_mb: int,
) -> tuple[bool, str | None]:
    response = client.fetch(
        url,
        headers={
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.1",
        },
    )

    if response.error:
        return False, response.error

    if response.status_code and response.status_code >= 400:
        return False, f"HTTP {response.status_code}"

    body = response.body or b""
    if not body:
        return False, "Empty body"

    max_bytes = max_pdf_mb * 1024 * 1024
    if len(body) > max_bytes:
        return False, f"PDF exceeds configured max size ({max_pdf_mb} MB)"

    content_type = (response.content_type or "").lower()
    is_pdf = body.startswith(b"%PDF") or "application/pdf" in content_type
    if not is_pdf:
        return False, f"Response is not a PDF (content-type={response.content_type})"

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(body)
    return True, None



def extract_text_from_pdf(path: Path, *, max_pages: int = 250) -> tuple[str | None, str | None]:
    try:
        reader = PdfReader(str(path))
    except Exception as exc:
        return None, f"Could not open PDF: {exc}"

    chunks: list[str] = []
    for idx, page in enumerate(reader.pages):
        if idx >= max_pages:
            break
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            chunks.append(text)

    full_text = "\n".join(chunks).strip()
    if not full_text:
        return None, "No extractable text"
    return full_text, None



def convert_pdf_to_pdfa(
    source_pdf: Path,
    destination_pdfa: Path,
    *,
    ghostscript_bin: str,
) -> tuple[bool, str | None]:
    destination_pdfa.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        ghostscript_bin,
        "-dPDFA=2",
        "-dBATCH",
        "-dNOPAUSE",
        "-dNOOUTERSAVE",
        "-sDEVICE=pdfwrite",
        "-dPDFACompatibilityPolicy=1",
        f"-sOutputFile={destination_pdfa}",
        str(source_pdf),
    ]

    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    except FileNotFoundError:
        return False, f"Ghostscript binary not found: {ghostscript_bin}"
    except Exception as exc:
        return False, str(exc)

    if proc.returncode != 0:
        stderr = proc.stderr.strip() or proc.stdout.strip() or "Ghostscript failed"
        return False, stderr

    if not destination_pdfa.exists() or destination_pdfa.stat().st_size == 0:
        return False, "Ghostscript finished but output PDF/A was not created"

    return True, None



def ensure_pdfa_copy(source_pdf: Path, destination_pdfa: Path) -> None:
    destination_pdfa.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_pdf, destination_pdfa)



def safe_file_stem(value: str) -> str:
    out = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")
    return out or "paper"



def _extract_arxiv_id(value: str) -> str | None:
    for regex in (ARXIV_ABS_RE, ARXIV_PDF_RE, DOI_ARXIV_RE):
        match = regex.search(value)
        if match:
            return match.group(1)
    return None
