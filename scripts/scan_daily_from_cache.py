#!/usr/bin/env python3
# PAL Daily Scan – v1.3.2 CRC-STRICT-PLUS
# Fokus: echte CRC-Range-Breakouts (maximale Selektion)

import os
import json
import pandas as pd
import numpy as np

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"

LOOKBACK_RANGE = 100
LOOKBACK_VOL   = 100
RISK_EUR       = 100
MAX_INVEST     = 4000
MIN_RISK_SHARE = 0.8


def today_utc():
    return pd.Timestamp.now("UTC").date()


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] < today_utc()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK_RANGE + LOOKBACK_VOL + 5:
            continue

        t0 = g.iloc[-1]
        hist = g.iloc[-(LOOKBACK_RANGE + 1):-1]

        # --- CRC Deckel (streng) ---
        top_highs = hist["High"].nlargest(5)
        deckel = top_highs.median()
        boden  = hist["Low"].min()
        range_pct = (deckel - boden) / deckel

        if range_pct < 0.06:
            continue

        touches = np.sum(np.abs(hist["High"] - deckel) / deckel < 0.002)
        if touches < 3:
            continue

        if t0["Close"] <= deckel:
            continue

        # --- Kompression ---
        ranges = hist["High"] - hist["Low"]
        atr14 = ranges.rolling(14).mean().iloc[-1]

        if atr14 / t0["Close"] > 0.02:
            continue

        if ranges.tail(5).max() > 1.2 * atr14:
            continue

        # --- Breakout Kerze ---
        rng = t0["High"] - t0["Low"]
        if rng <= 0:
            continue

        close_quality = (t0["Close"] - t0["Low"]) / rng
        if close_quality < 0.7 or t0["Close"] <= t0["Open"]:
            continue

        med_rng = (g.iloc[-(LOOKBACK_VOL+1):-1]["High"] -
                   g.iloc[-(LOOKBACK_VOL+1):-1]["Low"]).median()
        if rng < 1.5 * med_rng:
            continue

        # --- Risk ---
        entry = float(t0["Close"])
        stop  = float(t0["Low"])
        risk_per_share = entry - stop
        if risk_per_share < MIN_RISK_SHARE:
            continue

        shares = int(RISK_EUR / risk_per_share)
        invest = shares * entry
        if invest > MAX_INVEST or shares <= 0:
            continue

        target_2r = entry + 2 * risk_per_share

        rows.append({
            "symbol": sym,
            "date": str(t0["date"]),
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target_2R": round(target_2r, 2),
            "risk_per_share": round(risk_per_share, 2),
            "shares_for_100eur": shares,
            "invest_eur": round(invest, 0),
            "deckel": round(deckel, 2),
            "hi250": round(g.iloc[-251:-1]["High"].max(), 2)
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine CRC-STRICT-PLUS Treffer.")
        return

    out["deckel_break_pct"] = (out["entry"] - out["deckel"]) / out["deckel"]

    out.sort_values(
        by=["deckel_break_pct", "risk_per_share"],
        ascending=[False, False],
        inplace=True
    )

    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3.2_CRC_STRICT_PLUS",
            "hits": int(len(out)),
            "risk_model": "SL=SignalLow, TP=2R",
            "max_invest": MAX_INVEST
        }, f, indent=2)

    print(f"Done → {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()