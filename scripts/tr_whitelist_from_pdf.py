#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build Trade Republic STOCK whitelist (global) from PDF.

Env:
  TR_PDF_URL        (optional)
  TR_CACHE_PATH     (optional, default data/tr_stock_whitelist.csv)
  OPENFIGI_API_KEY  (optional but recommended)
"""

import os
import io
import re
import time
import json
import sys
import requests
import pandas as pd

PDF_URL = os.getenv(
    "TR_PDF_URL",
    "https://assets.traderepublic.com/assets/files/DE/Instrument_Universe_DE_en.pdf"
)

CACHE_PATH = os.getenv(
    "TR_CACHE_PATH",
    "data/tr_stock_whitelist.csv"
)

OF_KEY = os.getenv("OPENFIGI_API_KEY", "").strip()
MAX_RETRY = 4
MAX_BACKOFF = 12.0

UA = {"User-Agent": "Mozilla/5.0"}


# ---------------------------------------------------
# HTTP Helpers
# ---------------------------------------------------

def _get(url, timeout=60):
    backoff = 1.0
    for _ in range(MAX_RETRY):
        try:
            r = requests.get(url, timeout=timeout, headers=UA)
            if r.status_code == 429:
                time.sleep(min(MAX_BACKOFF, backoff))
                backoff *= 2
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            err = e
            time.sleep(min(MAX_BACKOFF, backoff))
            backoff *= 2
    raise err


def _post(url, payload, headers, timeout=60):
    backoff = 1.0
    for _ in range(MAX_RETRY):
        try:
            r = requests.post(url, data=payload, headers=headers, timeout=timeout)
            if r.status_code == 429:
                time.sleep(min(MAX_BACKOFF, backoff))
                backoff *= 2
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            err = e
            time.sleep(min(MAX_BACKOFF, backoff))
            backoff *= 2
    raise err


# ---------------------------------------------------
# OpenFIGI Mapping
# ---------------------------------------------------

def openfigi_batch_isin_to_ticker(isins):
    url = "https://api.openfigi.com/v3/mapping"
    headers = {"Content-Type": "application/json"}
    if OF_KEY:
        headers["X-OPENFIGI-APIKEY"] = OF_KEY

    batch_size = 100 if OF_KEY else 10
    out = {}

    for i in range(0, len(isins), batch_size):
        chunk = isins[i:i+batch_size]

        jobs = [{"idType": "ID_ISIN", "idValue": isin} for isin in chunk]

        try:
            resp = _post(url, json.dumps(jobs), headers).json()
        except Exception as e:
            print(f"[WARN] OpenFIGI chunk failed: {e}")
            continue

        for isin, job in zip(chunk, resp):
            rows = job.get("data") or []
            for r in rows:
                if r.get("marketSector") != "Equity":
                    continue
                ticker = r.get("ticker")
                if ticker:
                    out[isin] = ticker.upper()
                    break

        print(f"OpenFIGI progress: {min(i+batch_size,len(isins))}/{len(isins)}")
        time.sleep(0.25 if OF_KEY else 1.0)

    return out


# ---------------------------------------------------
# Main
# ---------------------------------------------------

def main():

    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber missing. pip install pdfplumber", file=sys.stderr)
        sys.exit(2)

    os.makedirs(os.path.dirname(CACHE_PATH) or ".", exist_ok=True)

    print("Downloading TR PDF...")
    buf = io.BytesIO(_get(PDF_URL).content)

    isin_re = re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b")

    isins = set()

    with pdfplumber.open(buf) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            found = isin_re.findall(text)
            for isin in found:
                isins.add(isin)

            if i % 25 == 0 or i == total:
                print(f"... parsed {i}/{total} pages")

    isins = sorted(isins)
    print(f"Total ISINs found: {len(isins)}")

    if not isins:
        print("No ISINs extracted. Aborting.")
        sys.exit(1)

    mapped = openfigi_batch_isin_to_ticker(isins)

    rows = []
    for isin in isins:
        ticker = mapped.get(isin, "")
        if ticker:
            rows.append({
                "isin": isin,
                "ticker": ticker
            })

    df = pd.DataFrame(rows).drop_duplicates("ticker")

    if df.empty:
        print("No tickers mapped. Aborting.")
        sys.exit(1)

    df.to_csv(CACHE_PATH, index=False)

    print(f"Done. Rows={len(df)} → {CACHE_PATH}")


if __name__ == "__main__":
    main()