#!/usr/bin/env python3
# PAL Daily Scan – v1.4 GREEN_LINE_RECLAIM_80D
# Fokus: echte CRC-Regime-Shifts über dynamische grüne Linie (Fib .382 inverted)

import os
import json
import pandas as pd
import numpy as np

# =========================
# SETTINGS
# =========================
LOOKBACK            = 250
MIN_DAYS_BELOW_LINE = 80
RISK_EUR            = 100
MAX_INVEST_EUR      = 4000

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"


def today_utc():
    return pd.Timestamp.utcnow().date()


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK + 30:
            continue

        last = g.iloc[-1]
        hist = g.iloc[-(LOOKBACK + 1):-1]

        # =========================
        # GREEN LINE (inverted Fib .382)
        # =========================
        hi = hist["High"].max()
        lo = hist["Low"].min()
        if hi <= lo:
            continue

        green_line = hi - 0.382 * (hi - lo)

        # =========================
        # TIME FILTER: days below green line
        # =========================
        days_below = 0
        for i in range(len(hist) - 1, -1, -1):
            if hist.iloc[i]["Close"] < green_line:
                days_below += 1
            else:
                break

        if days_below < MIN_DAYS_BELOW_LINE:
            continue

        # =========================
        # STRUCTURAL RECLAIM
        # =========================
        entry = float(last["Close"])
        if entry <= green_line:
            continue

        rng = float(last["High"]) - float(last["Low"])
        if rng <= 0:
            continue

        close_quality = (
            entry > float(last["Open"]) and
            entry >= float(last["Low"]) + 0.65 * rng
        )
        if not close_quality:
            continue

        # No spike
        if entry > green_line + 0.6 * rng:
            continue

        # =========================
        # RISK MODEL
        # =========================
        stop = float(last["Low"])
        risk_per_share = entry - stop
        if risk_per_share <= 0:
            continue

        shares = int(RISK_EUR / risk_per_share)
        invest = shares * entry

        if shares <= 0 or invest > MAX_INVEST_EUR:
            continue

        target_2r = entry + 2 * risk_per_share

        # =========================
        # ROOM TO 250D HIGH
        # =========================
        hi250 = hist["High"].max()
        reward = hi250 - entry
        rr = reward / risk_per_share
        if rr < 2.5:
            continue

        # =========================
        # VOLUME CONFIRMATION
        # =========================
        vol_hist = g.iloc[-21:-1]["Volume"]
        if len(vol_hist) < 10:
            continue

        rvol20 = float(last["Volume"]) / float(vol_hist.median())
        if rvol20 < 1.2:
            continue

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target_2R": round(target_2r, 2),
            "risk_per_share": round(risk_per_share, 2),
            "shares_for_100eur": shares,
            "invest_eur": round(invest, 0),
            "green_line": round(green_line, 2),
            "days_below_green": days_below,
            "rr_to_250d_high": round(rr, 2),
            "rvol20": round(rvol20, 2)
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine Treffer.")
        return

    out.sort_values(
        by=["days_below_green", "rvol20"],
        ascending=[False, False],
        inplace=True
    )

    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.4_GREEN_LINE_RECLAIM_80D",
            "hits": int(len(out)),
            "min_days_below_green": MIN_DAYS_BELOW_LINE,
            "risk_model": "SL=SignalLow, TP=2R",
            "max_invest": MAX_INVEST_EUR
        }, f, indent=2)

    print(f"Done → {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()