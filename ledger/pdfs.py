from __future__ import annotations

import functools
import re
import shutil
import subprocess
import logging
from pathlib import Path

from pypdf import PdfReader

from .models import Publication
from .net import HttpClient

ARXIV_ABS_RE = re.compile(r"arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)", re.IGNORECASE)
ARXIV_PDF_RE = re.compile(r"arxiv\.org/pdf/([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)\.pdf", re.IGNORECASE)
DOI_ARXIV_RE = re.compile(r"10\.48550/arXiv\.([0-9]{4}\.[0-9]{4,5}(?:v\d+)?)", re.IGNORECASE)

# Keep logs focused on actionable scan failures we return, not parser internals.
logging.getLogger("pypdf").setLevel(logging.ERROR)
logging.getLogger("pypdf._reader").setLevel(logging.ERROR)



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

    support_files, support_error = _pdfa_support_files(ghostscript_bin)
    if support_error:
        return False, support_error
    pdfa_def_template, icc_profile = support_files
    pdfa_def_path = destination_pdfa.parent / f".{destination_pdfa.stem}.PDFA_def.ps"
    pdfa_def_path.write_text(_render_pdfa_def(pdfa_def_template, icc_profile), encoding="utf-8")

    cmd = [
        ghostscript_bin,
        "-dPDFA=2",
        "-dBATCH",
        "-dNOPAUSE",
        "-dNOOUTERSAVE",
        "-sDEVICE=pdfwrite",
        "-sColorConversionStrategy=RGB",
        "-sProcessColorModel=DeviceRGB",
        "-dPDFACompatibilityPolicy=1",
        f"--permit-file-read={icc_profile}",
        f"--permit-file-read={pdfa_def_path}",
        f"-sOutputFile={destination_pdfa}",
        str(pdfa_def_path),
        str(source_pdf),
    ]

    try:
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

        validation_ok, validation_error = validate_pdfa(destination_pdfa)
        if not validation_ok:
            destination_pdfa.unlink(missing_ok=True)
            return False, validation_error or "PDF/A validation failed"

        return True, None
    finally:
        pdfa_def_path.unlink(missing_ok=True)



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


def validate_pdfa(path: Path) -> tuple[bool, str | None]:
    validator = shutil.which("verapdf")
    if validator:
        try:
            proc = subprocess.run(
                [validator, "--format", "text", str(path)],
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception as exc:
            return False, f"veraPDF execution failed: {exc}"

        output = (proc.stdout or "").strip()
        if proc.returncode == 0 and output.startswith("PASS "):
            return True, None
        detail = output or (proc.stderr or "").strip() or "veraPDF reported a validation failure"
        return False, detail

    data = path.read_bytes()
    required_markers = [
        b"pdfaid:part",
        b"pdfaid:conformance",
        b"OutputIntent",
    ]
    missing = [marker.decode("latin1") for marker in required_markers if marker not in data]
    if missing:
        return False, "PDF/A markers missing: " + ", ".join(missing)
    return True, None


@functools.lru_cache(maxsize=8)
def _pdfa_support_files(ghostscript_bin: str) -> tuple[tuple[Path, Path] | None, str | None]:
    binary = shutil.which(ghostscript_bin) or ghostscript_bin
    gs_path = Path(binary).expanduser().resolve()
    candidates: list[Path] = []

    for root in [
        gs_path.parent.parent / "share" / "ghostscript",
        Path("/opt/homebrew/share/ghostscript"),
        Path("/usr/local/share/ghostscript"),
        Path("/usr/share/ghostscript"),
        Path("/opt/homebrew/Cellar/ghostscript"),
        Path("/usr/local/Cellar/ghostscript"),
    ]:
        if root.exists():
            candidates.append(root)

    pdfa_def = _first_existing(candidates, "PDFA_def.ps")
    icc_profile = _first_existing(candidates, "srgb.icc")
    if pdfa_def is None or icc_profile is None:
        return None, "Ghostscript PDF/A support files not found (need PDFA_def.ps and srgb.icc)"
    return (pdfa_def, icc_profile), None


def _first_existing(roots: list[Path], filename: str) -> Path | None:
    for root in roots:
        direct = root / "lib" / filename
        if direct.exists():
            return direct
        direct = root / "iccprofiles" / filename
        if direct.exists():
            return direct
        matches = list(root.rglob(filename))
        if matches:
            return matches[0]
    return None


def _render_pdfa_def(template_path: Path, icc_profile: Path) -> str:
    template = template_path.read_text(encoding="utf-8")
    return re.sub(
        r"/ICCProfile\s+\(srgb\.icc\)",
        f"/ICCProfile ({icc_profile.as_posix()})",
        template,
        count=1,
    )
