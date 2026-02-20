#!/usr/bin/env python3
# PAL Daily Scan – v2.0 ACCEPTANCE BREAK
#
# Setup:
# Tag 0: Open < .382  AND Close > .382
# Tag 1: Gap Up (Open > Close Tag0) AND Close > Open
#
# Entry  = Close Tag1
# Stop   = Low Tag0
# Target = 2R
#
# OHLCV only

import os
import json
import pandas as pd
import numpy as np

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"

LOOKBACK_TOTAL = 250


def today_utc():
    return pd.Timestamp.utcnow().date()


def main():

    if not os.path.exists(IN_CSV):
        raise SystemExit("OHLCV Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] < today_utc()]

    rows = []

    for sym, g in df.groupby("symbol"):

        g = g.sort_values("date").reset_index(drop=True)

        if len(g) < LOOKBACK_TOTAL + 5:
            continue

        # Letzte 2 abgeschlossene Tage
        t1 = g.iloc[-1]      # Confirmation Day
        t0 = g.iloc[-2]      # Break Day

        # 250d Historie bis vor Break-Tag
        hist = g.iloc[-(LOOKBACK_TOTAL+2):-2]

        if len(hist) < LOOKBACK_TOTAL:
            continue

        # ============================================================
        # GREEN LINE (.382 vom 250d Range)
        # ============================================================

        hi = hist["High"].max()
        lo = hist["Low"].min()
        green_line = hi - 0.382 * (hi - lo)

        # ============================================================
        # TAG 0 – BREAK DAY
        # ============================================================

        cond_break = (
            (t0["Open"] < green_line) and
            (t0["Close"] > green_line)
        )

        if not cond_break:
            continue

        # ============================================================
        # TAG 1 – CONFIRMATION DAY
        # ============================================================

        cond_confirm = (
            (t1["Open"] > t0["Close"]) and     # Gap Up
            (t1["Close"] > t1["Open"])         # Bullish Close
        )

        if not cond_confirm:
            continue

        # ============================================================
        # ENTRY / STOP / TARGET
        # ============================================================

        entry = float(t1["Close"])
        stop = float(t0["Low"])
        risk = entry - stop

        if risk <= 0:
            continue

        target_2r = entry + 2 * risk

        rows.append({
            "symbol": sym,
            "date": str(t1["date"]),
            "setup_type": "ACCEPTANCE_BREAK",
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target_2R": round(target_2r, 2),
            "risk_per_share": round(risk, 2),
            "green_line": round(green_line, 2),
            "break_day_close": round(float(t0["Close"]), 2)
        })

    out = pd.DataFrame(rows)

    if out.empty:
        print("Keine ACCEPTANCE_BREAK Treffer.")
        return

    out.sort_values(
        by="risk_per_share",
        ascending=True,
        inplace=True
    )

    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v2.0_ACCEPTANCE_BREAK",
            "hits": int(len(out)),
            "logic": "Day0_break_above_382 + Day1_gap_confirmation"
        }, f, indent=2)

    print(f"Done → {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()