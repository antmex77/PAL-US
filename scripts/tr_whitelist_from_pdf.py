#!/usr/bin/env python3
# Build TR whitelist (US only)

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


# --------------------------------------------------
# Extract ISINs from PDF
# --------------------------------------------------

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


# --------------------------------------------------
# Map ISIN → US Ticker only
# --------------------------------------------------

def openfigi_batch_isin_to_us_ticker(isins, batch_size=100):

    us_rows = []

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
                if r.get("exchCode") == "US" and r.get("ticker"):
                    us_rows.append({
                        "isin": isin,
                        "ticker": r["ticker"],
                        "name": r.get("name", ""),
                        "exchCode": r.get("exchCode")
                    })
                    break  # nur erste US-Zuordnung

        print(f"... mapped {min(i+batch_size, len(isins))}/{len(isins)}")
        time.sleep(0.25)

    print(f"US stocks found: {len(us_rows)}")
    return us_rows


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    if not TR_PDF_URL:
        raise SystemExit("TR_PDF_URL not set")
    if not OPENFIGI_API_KEY:
        raise SystemExit("OPENFIGI_API_KEY not set")

    isins = extract_isins_from_pdf(TR_PDF_URL)
    us_data = openfigi_batch_isin_to_us_ticker(isins)

    if not us_data:
        raise SystemExit("No US stocks found")

    df = pd.DataFrame(us_data).drop_duplicates(subset=["ticker"])
    df = df.sort_values("ticker")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, index=False)

    print(f"Saved → {OUT_PATH}")
    print(f"Final US universe size: {len(df)}")


if __name__ == "__main__":
    main()