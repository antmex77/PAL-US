#!/usr/bin/env python3
# Build TR whitelist (US common stocks only)

import os
import re
import requests
import pdfplumber
import pandas as pd
import time

TR_PDF_URL = os.getenv("TR_PDF_URL")
OUT_PATH = os.getenv("TR_CACHE_PATH", "data/tr_stock_whitelist.csv")
OPENFIGI_API_KEY = os.getenv("OPENFIGI_API_KEY")

FIGI_URL = "https://api.openfigi.com/v3/mapping"

HEADERS = {
    "Content-Type": "application/json",
    "X-OPENFIGI-APIKEY": OPENFIGI_API_KEY
}


# -----------------------------
# Extract ISINs
# -----------------------------

def extract_isins_from_pdf(url):
    print("Downloading TR PDF...")
    r = requests.get(url)
    r.raise_for_status()

    isins = set()

    with pdfplumber.open(r.content) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            found = re.findall(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b", text)
            isins.update(found)
            if i % 25 == 0 or i == total:
                print(f"... parsed {i}/{total} pages")

    print(f"Total ISINs found: {len(isins)}")
    return list(isins)


# -----------------------------
# Map & filter properly
# -----------------------------

def openfigi_batch_filter_us_common(isins, batch_size=100):

    rows = []

    for i in range(0, len(isins), batch_size):

        batch = isins[i:i+batch_size]
        jobs = [{"idType": "ID_ISIN", "idValue": x} for x in batch]

        resp = requests.post(FIGI_URL, json=jobs, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        for isin, result in zip(batch, data):

            if "data" not in result:
                continue

            for r in result["data"]:

                if (
                    r.get("exchCode") == "US"
                    and r.get("marketSector") == "Equity"
                    and r.get("securityType") == "Common Stock"
                    and r.get("ticker")
                ):
                    rows.append({
                        "isin": isin,
                        "ticker": r["ticker"],
                        "name": r.get("name", "")
                    })
                    break

        print(f"... mapped {min(i+batch_size, len(isins))}/{len(isins)}")
        time.sleep(0.25)

    print(f"US Common Stocks found: {len(rows)}")
    return rows


# -----------------------------
# Main
# -----------------------------

def main():

    if not TR_PDF_URL:
        raise SystemExit("TR_PDF_URL not set")
    if not OPENFIGI_API_KEY:
        raise SystemExit("OPENFIGI_API_KEY not set")

    isins = extract_isins_from_pdf(TR_PDF_URL)
    rows = openfigi_batch_filter_us_common(isins)

    if not rows:
        raise SystemExit("No US common stocks found")

    df = pd.DataFrame(rows).drop_duplicates(subset=["ticker"])
    df = df.sort_values("ticker")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, index=False)

    print(f"Saved → {OUT_PATH}")
    print(f"Final US Common Stock universe size: {len(df)}")


if __name__ == "__main__":
    main()