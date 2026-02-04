#!/usr/bin/env python3
# PAL Daily Scan – v1.3 (SETUP B ONLY)
#
# SETUP B:
# Range Breakout with Acceptance
# - Mehrfach getestete Range-Highs
# - Echter Close-Break
# - Volumen-Bestätigung
#
# KEIN:
# - Base / grüne Linie
# - Moving Averages
# - Trend-Fortsetzung
#
# Ziel: Trades wie CRC, SNDA, AAPL (Aug)

import os
import json
import pandas as pd

# ---------------- CONFIG ----------------
LOOKBACK_MIN = 20
LOOKBACK_MAX = 60
MAX_RANGE_PCT = 0.18      # max 18 % Range
HIGH_TOL = 0.005          # 0.5 % Toleranz für Range-High-Tests
BREAK_BUFFER = 0.002      # 0.2 % echter Break
MAX_EXTENSION = 0.06      # Anti-Chase
MIN_VOL_MULT = 1.3

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"

# ----------------------------------------


def today_utc_date():
    return pd.Timestamp.utcnow().date()


def find_range(g):
    """
    Sucht eine valide Range im Fenster 20–60 Tage
    """
    for lb in range(LOOKBACK_MAX, LOOKBACK_MIN - 1, -1):
        win = g.iloc[-(lb + 1):-1]  # bis t-1
        hi = float(win["High"].max())
        lo = float(win["Low"].min())
        if lo <= 0:
            continue

        range_pct = (hi - lo) / lo
        if range_pct > MAX_RANGE_PCT:
            continue

        # Wie oft wurde das RangeHigh getestet?
        tests = win[abs(win["High"] - hi) / hi <= HIGH_TOL]
        if len(tests) < 3:
            continue

        # Kein vorheriger Close über RangeHigh
        if (win["Close"] > hi).any():
            continue

        return {
            "range_high": hi,
            "range_low": lo,
            "range_pct": round(range_pct, 3),
            "lookback": lb
        }

    return None


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("❌ OHLCV-Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK_MAX + 5:
            continue

        last = g.iloc[-1]   # t0
        prev = g.iloc[-2]   # t-1

        rng = find_range(g)
        if not rng:
            continue

        range_high = rng["range_high"]
        range_low  = rng["range_low"]

        # ---------- BREAKOUT ----------
        if not (float(last["Close"]) > range_high * (1 + BREAK_BUFFER)):
            continue

        # Anti-Chase
        extension = (float(last["Close"]) - range_high) / range_high
        if extension > MAX_EXTENSION:
            continue

        # ---------- VOLUME ----------
        vol_hist = g.iloc[-21:-1]["Volume"]
        if len(vol_hist) < 10:
            continue

        vol_med = float(vol_hist.median())
        if vol_med <= 0:
            continue

        vol_mult = float(last["Volume"]) / vol_med
        if vol_mult < MIN_VOL_MULT:
            continue

        # ---------- OUTPUT ----------
        range_height = range_high - range_low

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "close": round(float(last["Close"]), 2),
            "range_high": round(range_high, 2),
            "range_low": round(range_low, 2),
            "range_width_pct": rng["range_pct"],
            "lookback_days": rng["lookback"],
            "volume_multiple": round(vol_mult, 2),
            "entry_hint": round(range_high, 2),
            "stop_loss": round(range_low, 2),
            "tp1": round(range_high + range_height, 2),
            "tp2": round(range_high + 2 * range_height, 2),
            "setup": "B_RANGE_BREAKOUT"
        })

    os.makedirs("out", exist_ok=True)

    out = pd.DataFrame(rows)
    if out.empty:
        out.to_csv(OUT_CSV, index=False)
        with open("out/summary.json", "w") as f:
            json.dump({
                "version": "v1.3_setupB",
                "hits": 0,
                "note": "No valid range breakouts"
            }, f, indent=2)
        print("Keine Treffer.")
        return

    out.sort_values(
        ["volume_multiple", "range_width_pct"],
        ascending=[False, True],
        inplace=True
    )
    out.insert(0, "rank", range(1, len(out) + 1))
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3_setupB",
            "hits": int(len(out)),
            "setup": "Range Breakout with Acceptance",
            "green_line": False
        }, f, indent=2)

    print(f"Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()