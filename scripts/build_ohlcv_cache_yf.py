#!/usr/bin/env python3
# Baut täglichen OHLCV-Cache (EOD, letzte ~430 Kalendertage) für TR-Tickers
# Output: data/levels_cache_250d.csv  (long format: date,symbol,Open,High,Low,Close,Volume)

import os, sys, time
import pandas as pd

LOOKBACK_D = int(os.getenv("LOOKBACK", "250"))
PERIOD_D   = int(os.getenv("PERIOD_DAYS", "430"))  # Kalendertage
TR_CSV     = os.getenv("TR_CACHE_PATH", "data/tr_us_whitelist.csv")
OUT_CSV    = "data/levels_cache_250d.csv"

def _normalize_for_yf(t: str) -> str:
    return str(t).replace(".", "-").replace("/", "-").upper()

def _from_yf_to_orig(t: str) -> str:
    return str(t).replace("-", ".")

def load_universe() -> list[str]:
    if not os.path.exists(TR_CSV):
        raise SystemExit(f"TR-Whitelist fehlt: {TR_CSV}")
    df = pd.read_csv(TR_CSV)
    col = "ticker" if "ticker" in df.columns else None
    if not col:
        raise SystemExit(f"{TR_CSV} ohne Spalte 'ticker'")
    syms = sorted(set([s for s in df[col].astype(str) if s]))
    # Problemfälle filtern (Spaces, Slash)
    syms = [s for s in syms if "/" not in s and " " not in s]
    print(f"TR universe: {len(syms)}")
    return syms

def yf_download_batch(tickers: list[str]) -> pd.DataFrame:
    import yfinance as yf
    df = yf.download(
        tickers=" ".join(tickers),
        period=f"{PERIOD_D}d",
        interval="1d",
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False
    )
    return df

def main():
    syms = load_universe()
    if not syms:
        raise SystemExit("Universum leer.")

    # yfinance-Symbolform
    yf_syms_all = [_normalize_for_yf(s) for s in syms]

    chunks = []
    STEP = 180
    for i in range(0, len(yf_syms_all), STEP):
        batch = yf_syms_all[i:i+STEP]
        print(f"... {min(i+STEP, len(yf_syms_all))} / {len(yf_syms_all)}")
        df = yf_download_batch(batch)
        if df is None or df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            frames = []
            for s in batch:
                if s not in df.columns.get_level_values(0): 
                    continue
                sub = df[s].reset_index().rename(columns={"Date":"date"})
                sub["symbol"] = _from_yf_to_orig(s)
                frames.append(sub)
            if frames:
                chunks.append(pd.concat(frames, ignore_index=True))
        else:
            sub = df.reset_index().rename(columns={"Date":"date"})
            sub["symbol"] = _from_yf_to_orig(batch[0])
            chunks.append(sub)
        time.sleep(0.10)

    if not chunks:
        raise SystemExit("yfinance lieferte keine Daten.")

    allx = pd.concat(chunks, ignore_index=True)
    # Pflichtspalten absichern
    for c in ["Open","High","Low","Close","Volume"]:
        if c not in allx.columns: allx[c] = pd.NA

    # Datum tz-naiv und als echtes Datum
    allx["date"] = pd.to_datetime(allx["date"], utc=True).dt.date
    # Nur abgeschlossene Tage (sollten es ohnehin sein)
    allx = allx[["date","symbol","Open","High","Low","Close","Volume"]].dropna(how="any")
    allx = allx.sort_values(["symbol","date"])
    # Für Performance: nur letzte LOOKBACK+40 Tage pro Symbol behalten
    allx = allx.groupby("symbol").tail(LOOKBACK_D + 40)

    os.makedirs("data", exist_ok=True)
    allx.to_csv(OUT_CSV, index=False)
    print(f"Done -> {OUT_CSV} rows={len(allx)} symbols={allx['symbol'].nunique()}")

if __name__ == "__main__":
    main()
