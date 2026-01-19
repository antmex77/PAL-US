#!/usr/bin/env python3
# Daily GAP Scan – cache only
# Setup:
# - GAP über Breakout-Level
# - Entry = Close der GAP-Kerze
# - Stop  = Low der GAP-Kerze
# - RR >= 2.0
# - Score >= 70

import os, json
import pandas as pd
from datetime import date

LOOKBACK = 250
MIN_SCORE = 70
MIN_RR = 2.0

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"

def today_utc_date():
    return pd.Timestamp.utcnow().date()

def compute_score(rvol20, spread_est, close_strong) -> float:
    s = 0.0
    s += 55.0 * min(rvol20 / 3.0, 1.0)
    if close_strong:
        s += 15.0
    s -= min(spread_est * 100.0, 10.0)
    return round(max(0.0, min(100.0, s)), 2)

def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("OHLCV cache missing")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK + 22:
            continue

        t0   = g.iloc[-1]
        t1   = g.iloc[-2]
        win  = g.iloc[-(LOOKBACK+1):-1]

        hi250 = win["High"].max()
        breakout = hi250

        # GAP over breakout
        if not (t0["Open"] > breakout and t1["Close"] <= breakout):
            continue

        entry = float(t0["Close"])
        stop  = float(t0["Low"])
        R = entry - stop
        if R <= 0:
            continue

        max_reward = hi250 - entry
        RR = max_reward / R if R > 0 else 0.0
        if RR < MIN_RR:
            continue

        vol20 = g.iloc[-21:-1]["Volume"].median()
        rvol20 = t0["Volume"] / vol20 if vol20 > 0 else 0.0

        rng = t0["High"] - t0["Low"]
        close_strong = t0["Close"] >= (t0["Low"] + 0.7 * rng)
        spread_est = rng / max(t0["Close"], 1e-6)

        score = compute_score(rvol20, spread_est, close_strong)
        if score < MIN_SCORE:
            continue

        rows.append({
            "symbol": sym,
            "date": str(t0["date"]),
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "risk_R": round(R, 2),
            "RR_to_hi250": round(RR, 2),
            "hi250": round(hi250, 2),
            "gap_pct": round((t0["Open"] / breakout - 1) * 100, 2),
            "rvol20": round(rvol20, 2),
            "score": score
        })

    out = pd.DataFrame(rows).sort_values("score", ascending=False)
    out.insert(0, "rank", range(1, len(out)+1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json","w") as f:
        json.dump({
            "hits": len(out),
            "gap_only": True,
            "min_rr": MIN_RR,
            "min_score": MIN_SCORE
        }, f, indent=2)

    print(f"Done -> {OUT_CSV} ({len(out)} hits)")

if __name__ == "__main__":
    main()