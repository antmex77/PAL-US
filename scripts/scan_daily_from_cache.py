#!/usr/bin/env python3
# PAL Daily Scan – v1.3 SETUP B (FINAL)
#
# Setup B:
# - Breakout über Range-Deckel (implizit über hi250)
# - Entry  = Close der Signalkerze
# - Stop   = Low der Signalkerze
# - Target = 2R
# - Fixes Risiko pro Trade: 100 EUR
#
# Fokus: handelbare Trades wie CRC

import os
import json
import math
import pandas as pd

# ---------------- CONFIG ----------------
LOOKBACK = 250
RISK_EUR = 100.0

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"

# ----------------------------------------

def today_utc_date():
    return pd.Timestamp.utcnow().date()


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("❌ OHLCV-Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")

        if len(g) < LOOKBACK + 5:
            continue

        last = g.iloc[-1]   # Signalkerze (t0)
        prev = g.iloc[-2]

        win = g.iloc[-(LOOKBACK + 1):-1]
        hi250 = float(win["High"].max())
        lo250 = float(win["Low"].min())
        range250 = hi250 - lo250
        if range250 <= 0:
            continue

        # --- BREAKOUT BEDINGUNG ---
        if not (
            float(prev["Close"]) <= hi250 and
            float(last["Close"]) > hi250
        ):
            continue

        entry = float(last["Close"])
        stop  = float(last["Low"])
        r_per_share = entry - stop
        if r_per_share <= 0:
            continue

        # --- QUALITY FILTERS ---
        rng = float(last["High"]) - float(last["Low"])
        if rng <= 0:
            continue

        # Close stark (kein Docht-Exit)
        if entry < float(last["Low"]) + 0.65 * rng:
            continue

        # Kein Micro-Break
        if (entry - hi250) / range250 < 0.10:
            continue

        # --- RISIKO- & POSITIONSGRÖSSE ---
        shares = math.floor(RISK_EUR / r_per_share)
        if shares <= 0:
            continue

        capital_required = shares * entry
        tp_price = entry + 2.0 * r_per_share
        profit_at_tp = shares * (tp_price - entry)

        # --- VOLUME INFO ---
        vol_hist = g.iloc[-21:-1]["Volume"]
        vol20_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0.0
        rvol20 = float(last["Volume"]) / vol20_med if vol20_med > 0 else 0.0
        dollar_vol = entry * float(last["Volume"])

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),

            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "tp_2r": round(tp_price, 2),

            "r_per_share": round(r_per_share, 2),
            "shares_for_100eur_risk": shares,
            "capital_required_eur": round(capital_required, 0),
            "profit_at_2r_eur": round(profit_at_tp, 0),

            "hi250": round(hi250, 2),
            "range250": round(range250, 2),

            "rvol20": round(rvol20, 2),
            "dollar_vol_today": round(dollar_vol, 0)
        })

    out = pd.DataFrame(rows)

    if out.empty:
        print("Keine Treffer.")
        # leere Datei bewusst NICHT schreiben
        return

    # 🔥 Sinnvolle Sortierung
    out.sort_values(
        ["capital_required_eur", "rvol20"],
        ascending=[True, False],
        inplace=True
    )

    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3_setup_B_final",
            "hits": int(len(out)),
            "risk_per_trade_eur": RISK_EUR,
            "target_r": 2.0,
            "stop_rule": "low_of_signal_candle"
        }, f, indent=2)

    print(f"✅ Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()