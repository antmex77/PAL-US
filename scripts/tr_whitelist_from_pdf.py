#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build Trade Republic STOCK whitelist (ALL countries) from PDF.
"""

import os, io, re, time, json, sys
import requests
import pandas as pd

PDF_URL     = os.getenv("TR_PDF_URL", "https://assets.traderepublic.com/assets/files/DE/Instrument_Universe_DE_en.pdf")
CACHE_PATH  = os.getenv("TR_CACHE_PATH", "data/tr_stock_whitelist.csv")
OF_KEY      = os.getenv("OPENFIGI_API_KEY", "").strip()
MAX_RETRY   = int(os.getenv("MAX_RETRY", "4"))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "12"))

UA = {"User-Agent": "Mozilla/5.0"}

def _get(url, timeout=60):
    back = 1.0
    for _ in range(MAX_RETRY):
        try:
            r = requests.get(url, timeout=timeout, headers=UA)
            r.raise_for_status()
            return r
        except Exception:
            time.sleep(min(MAX_BACKOFF, back))
            back *= 2
    raise RuntimeError("Download failed")

def _post(url, data, headers=None, timeout=60):
    back = 1.0
    for _ in range(MAX_RETRY):
        try:
            r = requests.post(url, data=data, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception:
            time.sleep(min(MAX_BACKOFF, back))
            back *= 2
    raise RuntimeError("POST failed")

def openfigi_batch_isin_to_ticker(isins):
    url = "https://api.openfigi.com/v3/mapping"
    headers = {"Content-Type": "application/json"}
    if OF_KEY:
        headers["X-OPENFIGI-APIKEY"] = OF_KEY

    batch = 100 if OF_KEY else 10
    out = {}

    for i in range(0, len(isins), batch):
        chunk = isins[i:i+batch]
        jobs = [{"idType":"ID_ISIN","idValue":x}]  # no exchCode restriction

        resp = _post(url, json.dumps(jobs), headers=headers).json()

        for isin, job in zip(chunk, resp):
            rows = job.get("data") or []
            for r in rows:
                if r.get("marketSector") != "Equity":
                    continue
                tk = r.get("ticker")
                if tk:
                    out[isin] = tk.upper()
                    break

        time.sleep(0.25 if OF_KEY else 1.0)
        print(f"OpenFIGI progress: {min(i+batch,len(isins))}/{len(isins)}")

    return out

def main():
    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber missing. pip install pdfplumber", file=sys.stderr)
        sys.exit(2)

    os.makedirs(os.path.dirname(CACHE_PATH) or ".", exist_ok=True)

    print("Downloading TR PDF...")
    buf = io.BytesIO(_get(PDF_URL).content)

    # Generic ISIN regex (2 letters + 9 alnum + 1 digit)
    isin_re = re.compile(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b")

    isins = set()

    with pdfplumber.open(buf) as pdf:
        total = len(pdf.pages)
        for p, page in enumerate(pdf.pages, start=1):
            txt = page.extract_text() or ""
            for m in isin_re.finditer(txt):
                isins.add(m.group(0))
            if p % 25 == 0 or p == total:
                print(f"... parsed {p}/{total} pages")

    isins = sorted(isins)
    print(f"Total ISINs found: {len(isins)}")

    mapped = openfigi_batch_isin_to_ticker(isins)

    rows = []
    for isin in isins:
        tk = mapped.get(isin, "")
        if tk:
            rows.append({"isin": isin, "ticker": tk})

    df = pd.DataFrame(rows).drop_duplicates("ticker")
    df = df[df["ticker"].str.match(r"^[A-Z0-9.\-]+$", na=False)]

    df.to_csv(CACHE_PATH, index=False)
    print(f"Done. STOCK whitelist rows={len(df)} -> {CACHE_PATH}")

if __name__ == "__main__":
    main()