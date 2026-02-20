# note: OCRmyPDF is a powerful tool that will take a long time to run, so we only use it as a fallback when native text extraction fails to find the fields we need. 

import os
import re
import subprocess
import tempfile
from typing import Optional, Dict

from pypdf import PdfReader


# Native extraction (useful for non-name fields)
def extract_text_native(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    chunks = []
    for page in reader.pages:
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        if t.strip():
            chunks.append(t)
    return "\n".join(chunks)


# OCRmyPDF sidecar extraction
# all data has Well Name and Number field, so we process as an image and have higher quality scans to make sure that there is data for that field
def extract_text_ocrmypdf_sidecar(pdf_path: str) -> str:
    with tempfile.TemporaryDirectory() as tmp:
        sidecar_path = os.path.join(tmp, "sidecar.txt")
        out_pdf = os.path.join(tmp, "ocr.pdf")

        cmd = [
            "ocrmypdf",
            "--skip-text",
            "--output-type", "pdf",
            "--sidecar", sidecar_path,
            "--oversample", "300",
            pdf_path,
            out_pdf,
        ]

        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if os.path.exists(sidecar_path):
            with open(sidecar_path, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        return ""


# Parsing helper functions
def normalize_ws(s: str) -> str:
    return re.sub(r"[ \t]+", " ", s).strip()


def extract_well_name_and_number(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"Well Name and Number\s*\n?\s*(.+)", text, flags=re.IGNORECASE)
    return normalize_ws(m.group(1)) if m else None


def normalize_api10_from_labeled_context(text: str) -> Optional[str]:
    # extract API only when it's near an API label (avoid phone numbers).
    if not text:
        return None

    m = re.search(
        r"\bAPI(?:\s*(?:NO\.|NUMBER|#))?\s*[:#]?\s*([0-9]{2}[\s\-]?[0-9]{3}[\s\-]?[0-9]{5})\b",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        return None

    digits = re.sub(r"\D", "", m.group(1))
    return digits if len(digits) == 10 else None


def extract_texts(pdf_path: str) -> Dict[str, str]:
    native = extract_text_native(pdf_path)

    try:
        ocr = extract_text_ocrmypdf_sidecar(pdf_path)
    except Exception:
        ocr = ""

    return {"native": native, "ocr": ocr}


def parse_fields(pdf_path: str) -> dict:
    texts = extract_texts(pdf_path)

    native_text = extract_text_native(pdf_path)
    well_name_and_number = extract_well_name_and_number(native_text)
    api10 = normalize_api10_from_labeled_context(native_text)

    ocr_text = ""
    if well_name_and_number is None:
        try:
            ocr_text = extract_text_ocrmypdf_sidecar(pdf_path)
        except Exception:
            ocr_text = ""

        # fill WELL NAME from OCR if possible
        well_name_and_number = extract_well_name_and_number(ocr_text)

        # if API missing from native, try OCR (still labeled-context only)
        if api10 is None and ocr_text:
            api10 = normalize_api10_from_labeled_context(ocr_text)

    return {
        "well_name_and_number": well_name_and_number,
        "api_number_10": api10, # may be None if no labeled API exists in the PDF
    }


if __name__ == "__main__":
    pdf_path = "data/W25160.pdf"
    result = parse_fields(pdf_path)
    print(result)