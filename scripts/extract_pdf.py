# extracts 
import os
import re
from typing import Dict, Optional
from pypdf import PdfReader


# pdf to text

def extract_text(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    chunks = []

    for i, page in enumerate(reader.pages):
        try:
            page_text = page.extract_text() or ""
        except Exception as e:
            # add failsafe in case there are issues with text extraction on a page
            print(f"Error extracting text from page {i} of {pdf_path}: {e}")
            page_text = ""

        if page_text.strip():
            chunks.append(page_text)

    return "\n".join(chunks)


# text extraction (based on W25160.pdf structure)

def find(pattern: str, text: str) -> Optional[str]:
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1).strip() if match else None


def parse_well_data(text: str) -> Dict:
    data = {}

    data["well_name"] = find(r"Well Name and Number\s+(.+)", text)
    data["operator"] = find(r"OPERATOR\s+(.+)", text)
    data["api_number"] = find(r"API NUMBER\s+([\d\-]+)", text)
    data["county_state"] = find(r"COUNTY/STATE\s+(.+)", text)
    data["field"] = find(r"FIELD\s+(.+)", text)

    # Completion date
    data["completion_date"] = find(r"Date Well Completed.*?\n.*?(\d+/\d+/\d+)", text)

    # Stimulation block (from page 8)
    data["date_stimulated"] = find(r"Date Stimulated\s+(\d+/\d+/\d+)", text)
    data["stimulated_formation"] = find(r"Date Stimulated.*?\n.*?([A-Za-z]+)\s+\d+", text)
    data["top_ft"] = find(r"Date Stimulated.*?\n.*?(\d{4,5})\s+\d{4,5}", text)
    data["bottom_ft"] = find(r"Date Stimulated.*?\n.*?\d{4,5}\s+(\d{4,5})", text)
    data["stimulation_stages"] = find(r"Date Stimulated.*?\n.*?\d{4,5}\s+\d{4,5}\s+(\d+)", text)

    data["volume"] = find(r"Volume\s+(\d+)", text)
    data["volume_units"] = find(r"Volume Units\s+([A-Za-z]+)", text)
    data["type_treatment"] = find(r"Type Treatment\s+([A-Za-z\s]+)", text)

    return data


def process_pdf(pdf_path: str) -> Dict:
    text = extract_text(pdf_path)
    return parse_well_data(text)


if __name__ == "__main__":
    pdf_path = "data/W25160.pdf" # correctly grabs api number, operator, county state, etc
    result = process_pdf(pdf_path)
    print(result)