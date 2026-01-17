#!/usr/bin/env python3
import os, io, re, time, json, sys
import requests, pandas as pd

PDF_URL     = os.getenv("TR_PDF_URL", "https://assets.traderepublic.com/assets/files/DE/Instrument_Universe_DE_en.pdf")
CACHE_PATH  = os.getenv("TR_CACHE_PATH", "data/tr_us_whitelist.csv")

# Optional, erhöht Trefferqoute beim ISIN->Ticker Mapping:
OF_KEY      = os.getenv("OPENFIGI_API_KEY", "")

MAX_RETRY   = int(os.getenv("MAX_RETRY", "4"))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "8"))
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120 Safari/537.36"}

def _get(url, params=None, timeout=60, headers=None):
    back, last = 1.0, None
    for _ in range(MAX_RETRY):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=headers or UA)
            if r.status_code == 429:
                last = RuntimeError("429"); time.sleep(min(MAX_BACKOFF, back)); back *= 2; continue
            r.raise_for_status(); return r
        except Exception as e:
            last = e; time.sleep(min(MAX_BACKOFF, back)); back *= 2
    raise last

def _post(url, data, headers=None, timeout=60):
    back, last = 1.0, None
    for _ in range(MAX_RETRY):
        try:
            r = requests.post(url, data=data, headers=headers, timeout=timeout)
            if r.status_code == 429:
                last = RuntimeError("429"); time.sleep(min(MAX_BACKOFF, back)); back *= 2; continue
            r.raise_for_status(); return r
        except Exception as e:
            last = e; time.sleep(min(MAX_BACKOFF, back)); back *= 2
    raise last

def openfigi_batch_isin_to_ticker(isins: list[str]) -> dict[str,str]:
    if not isins: return {}
    url = "https://api.openfigi.com/v3/mapping"
    headers = {"Content-Type": "application/json"}
    if OF_KEY: headers["X-OPENFIGI-APIKEY"] = OF_KEY
    batch = 100 if OF_KEY else 10
    out = {}
    def pick(rows):
        allowed = {None,"US","NYS","NYQ","NAS","NSQ","ASE","ARC","BATS","IEXG"}
        best = None
        for r in rows:
            if r.get("marketSector")!="Equity": continue
            if r.get("exchCode") not in allowed: continue
            tk = r.get("ticker"); st = (r.get("securityType") or "") + "|" + (r.get("securityType2") or "")
            if not tk: continue
            if ("Common" in st) or ("Depositary" in st) or ("ADR" in st): return tk
            best = best or tk
        return best
    for i in range(0, len(isins), batch):
        chunk = isins[i:i+batch]
        jobs = [{"idType":"ID_ISIN","idValue":x,"exchCode":"US","marketSecDes":"Equity"} for x in chunk]
        try:
            resp = _post(url, data=json.dumps(jobs), headers=headers).json()
        except Exception as e:
            print(f"[WARN] OpenFIGI chunk fail: {e}"); continue
        for isin, job in zip(chunk, resp):
            rows = job.get("data") or []
            tk = pick(rows)
            if tk: out[isin] = tk
        time.sleep(0.25 if OF_KEY else 1.2)
    return out

def main():
    try:
        import pdfplumber
    except ImportError:
        print("pdfplumber missing. Run: pip install pdfplumber", file=sys.stderr)
        sys.exit(2)

    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    print(f"Downloading TR PDF -> {PDF_URL}")
    buf = io.BytesIO(_get(PDF_URL).content)

    us_re = re.compile(r"\bUS[A-Z0-9]{9}\d\b")
    isins, seen = [], set()
    with pdfplumber.open(buf) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            txt = page.extract_text() or ""
            for m in us_re.finditer(txt):
                isin = m.group(0)
                if isin not in seen:
                    seen.add(isin); isins.append(isin)
            if i % 25 == 0: print(f"... parsed {i}/{len(pdf.pages)} pages")
    isins = sorted(set(isins))
    print(f"US ISINs: {len(isins)}")

    # ISIN -> Ticker (OpenFIGI)
    mapped = openfigi_batch_isin_to_ticker(isins)

    rows = []
    for isin in isins:
        tk = mapped.get(isin, "")
        if tk: rows.append({"isin": isin, "ticker": tk})

    df = pd.DataFrame(rows).drop_duplicates("ticker")
    # Nur saubere ASCII-Ticker
    df = df[df["ticker"].astype(str).str.isascii()]
    df.to_csv(CACHE_PATH, index=False)
    print(f"Done. rows={len(df)} -> {CACHE_PATH}")

if __name__ == "__main__":
    main()
