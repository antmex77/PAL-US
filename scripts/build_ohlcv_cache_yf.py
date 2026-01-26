#!/usr/bin/env python3
# Build OHLCV Cache v2.1 (EOD, weekday-aware)
# Output: data/levels_cache_250d.csv
#
# FIXES:
# - Börsentags-logische Staleness-Prüfung (kein Weekend-Bug mehr)
# - Explizites Logging der letzten Kerze
# - Harter Abbruch bei echten Stale-Daten
#
# Datenquelle: yfinance (EOD)

import os
import time
import sys
from datetime import timedelta
import pandas as pd

LOOKBACK_D = int(os.getenv("LOOKBACK", "250"))
PERIOD_D   = int(os.getenv("PERIOD_DAYS", "430"))
TR_CSV     = os.getenv("TR_CACHE_PATH", "data/tr_us_whitelist.csv")
OUT_CSV    = "data/levels_cache_250d.csv"

# -------------------------
# Helpers
# -------------------------

def today_utc_date():
    return pd.Timestamp.now("UTC").date()

def last_expected_trading_day(today):
    """
    US equities:
    - Mon -> Fri
    - Sat -> Fri
    - Sun -> Fri
    - Tue–Fri -> Yesterday
    """
    wd = today.weekday()  # Mon=0 ... Sun=6
    if wd == 0:   # Monday
        return today - timedelta(days=3)
    if wd == 5:   # Saturday
        return today - timedelta(days=1)
    if wd == 6:   # Sunday
        return today - timedelta(days=2)
    return today - timedelta(days=1)

def normalize_for_yf(t: str) -> str:
    return str(t).replace(".", "-").replace("/", "-").upper()

def from_yf_to_orig(t: str) -> str:
    return str(t).replace("-", ".")

# -------------------------
# Load universe
# -------------------------

def load_universe() -> list[str]:
    if not os.path.exists(TR_CSV):
        raise SystemExit(f"❌ TR whitelist fehlt: {TR_CSV}")

    df = pd.read_csv(TR_CSV)
    if "ticker" not in df.columns:
        raise SystemExit(f"❌ {TR_CSV} ohne Spalte 'ticker'")

    syms = sorted(set(df["ticker"].astype(str)))
    syms = [s for s in syms if s and "/" not in s and " " not in s]

    print(f"Universe loaded: {len(syms)} symbols")
    return syms

# -------------------------
# Download
# -------------------------

def yf_download_batch(tickers: list[str]) -> pd.DataFrame:
    import yfinance as yf
    return yf.download(
        tickers=" ".join(tickers),
        period=f"{PERIOD_D}d",
        interval="1d",
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False
    )

# -------------------------
# Main
# -------------------------

def main():
    today = today_utc_date()
    expected_last_bar = last_expected_trading_day(today)

    syms = load_universe()
    if not syms:
        raise SystemExit("❌ Universum leer")

    yf_syms = [normalize_for_yf(s) for s in syms]

    chunks = []
    STEP = 120

    print(f"Downloading OHLCV up to {today} (expected last bar ≥ {expected_last_bar})")

    for i in range(0, len(yf_syms), STEP):
        batch = yf_syms[i:i+STEP]
        print(f"... {min(i+STEP, len(yf_syms))} / {len(yf_syms)}")

        df = yf_download_batch(batch)
        if df is None or df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            frames = []
            for s in batch:
                if s not in df.columns.get_level_values(0):
                    continue
                sub = df[s].reset_index().rename(columns={"Date": "date"})
                sub["symbol"] = from_yf_to_orig(s)
                frames.append(sub)
            if frames:
                chunks.append(pd.concat(frames, ignore_index=True))
        else:
            sub = df.reset_index().rename(columns={"Date": "date"})
            sub["symbol"] = from_yf_to_orig(batch[0])
            chunks.append(sub)

        time.sleep(0.1)

    if not chunks:
        raise SystemExit("❌ yfinance lieferte keine Daten")

    allx = pd.concat(chunks, ignore_index=True)

    # Pflichtspalten absichern
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c not in allx.columns:
            raise SystemExit(f"❌ Fehlende Spalte: {c}")

    # Datum normalisieren
    allx["date"] = pd.to_datetime(allx["date"], utc=True).dt.date
    allx = allx[["date","symbol","Open","High","Low","Close","Volume"]].dropna(how="any")
    allx = allx.sort_values(["symbol", "date"])

    # Nur relevante Historie behalten
    allx = allx.groupby("symbol").tail(LOOKBACK_D + 40)

    # -------------------------
    # STALENESS CHECK
    # -------------------------

    latest_bar = allx["date"].max()
    print(f"Latest bar in cache: {latest_bar}")

    if latest_bar < expected_last_bar:
        print(f"❌ STALE DATA: latest={latest_bar}, expected>={expected_last_bar}")
        sys.exit(1)

    # -------------------------
    # Write cache
    # -------------------------

    os.makedirs("data", exist_ok=True)
    allx.to_csv(OUT_CSV, index=False)

    print(f"✅ Done -> {OUT_CSV}")
    print(f"Rows: {len(allx)} | Symbols: {allx['symbol'].nunique()}")

# -------------------------

if __name__ == "__main__":
    main()