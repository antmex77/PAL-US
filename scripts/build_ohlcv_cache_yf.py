#!/usr/bin/env python3
# Cache v2 – robuster OHLCV Builder (Yahoo Finance, EOD only)
# FIX: verhindert eingefrorene Daten durch period+threads Bug

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta, date

LOOKBACK_D = int(os.getenv("LOOKBACK", "250"))
TR_CSV     = os.getenv("TR_CACHE_PATH", "data/tr_us_whitelist.csv")
OUT_CSV    = "data/levels_cache_250d.csv"

# ---------------------------- helpers ----------------------------

def today_utc_date():
    return pd.Timestamp.utcnow().date()

def yf_symbol(sym: str) -> str:
    return sym.replace(".", "-").replace("/", "-").upper()

def from_yf(sym: str) -> str:
    return sym.replace("-", ".")

def load_universe() -> list[str]:
    if not os.path.exists(TR_CSV):
        raise SystemExit(f"Missing whitelist: {TR_CSV}")

    df = pd.read_csv(TR_CSV)
    if "ticker" not in df.columns:
        raise SystemExit("Whitelist ohne Spalte 'ticker'")

    syms = (
        df["ticker"]
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    # harte Filter
    syms = [s for s in syms if s and " " not in s and "/" not in s]

    print(f"Universe loaded: {len(syms)} symbols")
    return sorted(syms)

# ---------------------------- main logic ----------------------------

def download_batch(tickers: list[str], start, end) -> pd.DataFrame:
    import yfinance as yf

    return yf.download(
        tickers=" ".join(tickers),
        start=start,
        end=end,
        interval="1d",
        auto_adjust=False,
        group_by="ticker",
        threads=False,     # <<< WICHTIG
        progress=False
    )

def main():
    syms = load_universe()
    if not syms:
        raise SystemExit("Universe leer.")

    start = today_utc_date() - timedelta(days=500)
    end   = today_utc_date()

    yf_syms = [yf_symbol(s) for s in syms]

    chunks = []
    STEP = 120

    print(f"Downloading OHLCV from {start} to {end}")

    for i in range(0, len(yf_syms), STEP):
        batch = yf_syms[i:i+STEP]
        print(f"... {i+len(batch)} / {len(yf_syms)}")

        df = download_batch(batch, start, end)
        if df is None or df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            for s in batch:
                if s not in df.columns.get_level_values(0):
                    continue
                sub = df[s].reset_index()
                sub.rename(columns={"Date": "date"}, inplace=True)
                sub["symbol"] = from_yf(s)
                chunks.append(sub)
        else:
            sub = df.reset_index()
            sub.rename(columns={"Date": "date"}, inplace=True)
            sub["symbol"] = from_yf(batch[0])
            chunks.append(sub)

        time.sleep(0.15)

    if not chunks:
        raise SystemExit("Yahoo lieferte keine Daten.")

    allx = pd.concat(chunks, ignore_index=True)

    # Pflichtspalten
    need = ["Open", "High", "Low", "Close", "Volume"]
    for c in need:
        if c not in allx.columns:
            raise SystemExit(f"Fehlende Spalte: {c}")

    allx["date"] = pd.to_datetime(allx["date"], utc=True).dt.date
    allx = allx.dropna(subset=["date","Open","High","Low","Close","Volume"])
    allx = allx.sort_values(["symbol","date"])

    # nur benötigte Länge
    allx = allx.groupby("symbol").tail(LOOKBACK_D + 40)

    # ------------------ HARD STALENESS CHECK ------------------

    max_date = allx["date"].max()
    expected = today_utc_date() - timedelta(days=1)

    print(f"Latest bar in cache: {max_date}")

    if max_date < expected:
        raise SystemExit(
            f"❌ STALE DATA: latest={max_date}, expected>={expected}"
        )

    # ----------------------------------------------------------

    os.makedirs("data", exist_ok=True)
    allx.to_csv(OUT_CSV, index=False)

    print(
        f"Done -> {OUT_CSV} | rows={len(allx)} | symbols={allx['symbol'].nunique()}"
    )

if __name__ == "__main__":
    main()