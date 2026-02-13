#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build Trade Republic US whitelist from PDF (no Polygon needed).

Env:
  TR_PDF_URL      (optional, default points to TR PDF)
  TR_CACHE_PATH   (optional, default data/tr_stock_whitelist.csv)
  OPENFIGI_API_KEY (optional but recommended: faster & better mapping)
"""

import os, io, re, time, json, sys
import requests
import pandas as pd

PDF_URL     = os.getenv("TR_PDF_URL", "https://assets.traderepublic.com/assets/files/DE/Instrument_Universe_DE_en.pdf")
CACHE_PATH  = os.getenv("TR_CACHE_PATH", "data/tr_stock_whitelist.csv")
OF_KEY      = os.getenv("OPENFIGI_API_KEY", "").strip()
MAX_RETRY   = int(os.getenv("MAX_RETRY", "4"))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "12"))

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120 Safari/537.36"}

def _get(url, params=None, timeout=60, headers=None):
    back, last = 1.0, None
    for _ in range(MAX_RETRY):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=headers or UA)
            if r.status_code == 429:
                last = RuntimeError("429")
                time.sleep(min(MAX_BACKOFF, back)); back *= 2; continue
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            time.sleep(min(MAX_BACKOFF, back)); back *= 2
    raise last

def _post(url, data, headers=None, timeout=60):
    back, last = 1.0, None
    for _ in range(MAX_RETRY):
        try:
            r = requests.post(url, data=data, headers=headers, timeout=timeout)
            if r.status_code == 429:
                last = RuntimeError("429")
                time.sleep(min(MAX_BACKOFF, back)); back *= 2; continue
            r.raise_for_status()
            return r
        except Exception as e:
            last = e
            time.sleep(min(MAX_BACKOFF, back)); back *= 2
    raise last

def openfigi_batch_isin_to_ticker(isins: list[str]) -> dict[str,str]:
    """Map ISIN -> ticker via OpenFIGI (US common/ADR preferred)."""
    url = "https://api.openfigi.com/v3/mapping"
    headers = {"Content-Type": "application/json"}
    if OF_KEY:
        headers["X-OPENFIGI-APIKEY"] = OF_KEY
    batch = 100 if OF_KEY else 10
    out = {}

    def pick(rows):
        allowed = {None,"US","NYS","NYQ","NAS","NSQ","ASE","ARC","BATS","IEXG"}
        best = None
        for r in rows:
            if r.get("marketSector") != "Equity":
                continue
            if r.get("exchCode") not in allowed:
                continue
            tk = r.get("ticker")
            if not tk:
                continue
            st = (r.get("securityType") or "") + "|" + (r.get("securityType2") or "")
            if ("Common" in st) or ("Depositary" in st) or ("ADR" in st):
                return tk
            best = best or tk
        return best

    for i in range(0, len(isins), batch):
        chunk = isins[i:i+batch]
        jobs = [{"idType":"ID_ISIN","idValue":x,"exchCode":"US","marketSecDes":"Equity"} for x in chunk]
        try:
            resp = _post(url, data=json.dumps(jobs), headers=headers).json()
        except Exception as e:
            print(f"[WARN] OpenFIGI chunk fail: {e}")
            continue
        for isin, job in zip(chunk, resp):
            rows = job.get("data") or []
            tk = pick(rows)
            if tk:
                out[isin] = tk
        time.sleep(0.25 if OF_KEY else 1.2)
        print(f"OpenFIGI progress: {min(i+batch,len(isins))}/{len(isins)} mapped")
    return out

def main():
    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber missing. pip install pdfplumber", file=sys.stderr)
        sys.exit(2)

    os.makedirs(os.path.dirname(CACHE_PATH) or ".", exist_ok=True)

    print(f"Downloading TR PDF -> {PDF_URL}")
    buf = io.BytesIO(_get(PDF_URL).content)

    us_re = re.compile(r"\bUS[A-Z0-9]{9}\d\b")
    isins, seen_cus = [], set()

    with pdfplumber.open(buf) as pdf:
        total = len(pdf.pages)
        for p, page in enumerate(pdf.pages, start=1):
            txt = page.extract_text() or ""
            for m in us_re.finditer(txt):
                isin = m.group(0)
                cus = isin[2:11]
                if cus in seen_cus:
                    continue
                seen_cus.add(cus)
                isins.append(isin)
            if p % 25 == 0 or p == total:
                print(f"... parsed {p}/{total} pages")

    isins = sorted(set(isins))
    print(f"US ISINs: {len(isins)}")

    mapped = openfigi_batch_isin_to_ticker(isins)

    rows = []
    for isin in isins:
        cus = isin[2:11]
        tk = mapped.get(isin, "")
        rows.append({"isin": isin, "cusip": cus, "ticker": (tk or "").strip().upper()})

    df = pd.DataFrame(rows).drop_duplicates("cusip")
    df["ticker"] = df["ticker"].fillna("").str.strip().str.upper()
    mask_ascii = df["ticker"].str.match(r"^[A-Z0-9.\-]+$", na=False)
    df = df[mask_ascii & (df["ticker"] != "")].copy()

    df.to_csv(CACHE_PATH, index=False)
    print(f"Done. whitelist rows={len(df)} -> {CACHE_PATH}")

if __name__ == "__main__":
    main()