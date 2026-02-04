#!/usr/bin/env python3
# PAL Daily Scan – v1.3 CRC FINAL
#
# Setup:
# - CRC / Range Breakout
# - Lokaler Deckel (nicht hi250!)
# - SL = Low der Signalkerze
# - TP = 2R
# - €-Sizing für 100 € Risiko
#
# Entry: Close(t0)
# Stop : Low(t0)
# Target: Entry + 2R

import os
import json
import pandas as pd

LOOKBACK_TREND = 250
LOOKBACK_RANGE = 40
RISK_EUR = 100

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"


def today_utc():
    return pd.Timestamp.utcnow().date()


def main():
    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] < today_utc()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK_TREND + LOOKBACK_RANGE:
            continue

        last = g.iloc[-1]     # signal candle
        prev = g.iloc[-2]

        trend_win = g.iloc[-(LOOKBACK_TREND + 1):-1]
        hi250 = trend_win["High"].max()
        lo250 = trend_win["Low"].min()

        # --- Trendfilter ---
        if prev["Close"] < hi250 * 0.75:
            continue

        # --- CRC Deckel ---
        range_win = g.iloc[-(LOOKBACK_RANGE + 1):-1]
        deckel = range_win["High"].max()

        # Touches am Deckel
        touches = (range_win["High"] >= deckel * 0.99).sum()
        if touches < 2:
            continue

        # --- Breakout ---
        if not (
            prev["Close"] <= deckel and
            last["Close"] > deckel
        ):
            continue

        # --- Kerzenqualität ---
        rng = last["High"] - last["Low"]
        if rng <= 0:
            continue

        if last["Close"] < last["Low"] + 0.7 * rng:
            continue

        entry = last["Close"]
        stop = last["Low"]
        r = entry - stop
        if r <= 0:
            continue

        target = entry + 2 * r

        # --- €-Sizing ---
        shares = RISK_EUR / r
        invest_eur = shares * entry

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target_2R": round(target, 2),
            "risk_per_share": round(r, 2),
            "shares_for_100eur": int(shares),
            "invest_eur": round(invest_eur, 0),
            "deckel": round(deckel, 2),
            "hi250": round(hi250, 2)
        })

    out = pd.DataFrame(rows)

    if out.empty:
        print("Keine Treffer.")
        return

    out.sort_values("invest_eur", inplace=True)
    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3_crc_final",
            "hits": int(len(out)),
            "setup": "CRC",
            "risk_eur": RISK_EUR,
            "rr": "2R"
        }, f, indent=2)

    print(f"Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()