#!/usr/bin/env python3
# PAL Daily Scan – v1.5 CRC-REGIME-SHIFT
# Fokus: Regime-Wechsel über grüne Linie mit News-Impulse-Proxy

import os
import json
import pandas as pd
import numpy as np

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"

LOOKBACK_GREEN = 250
MIN_DAYS_BELOW_GREEN = 80

LOOKBACK_VOL = 20
RISK_EUR = 100
MAX_INVEST = 4000


def today_utc():
    return pd.Timestamp.now("UTC").date()


def compute_green_line(win):
    hi = win["High"].max()
    lo = win["Low"].min()
    return hi - 0.382 * (hi - lo)


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] < today_utc()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK_GREEN + 5:
            continue

        last = g.iloc[-1]
        win  = g.iloc[-(LOOKBACK_GREEN + 1):-1]

        green = compute_green_line(win)

        closes = win["Close"]
        below_green_days = np.sum(closes < green)

        if below_green_days < MIN_DAYS_BELOW_GREEN:
            continue

        # --- FIRST ACCEPTED BREAK ---
        if last["Close"] <= green:
            continue

        if win.iloc[-1]["Close"] > green:
            continue  # kein Erstüberstieg

        rng = last["High"] - last["Low"]
        if rng <= 0:
            continue

        close_quality = (
            last["Close"] >= last["Open"] and
            last["Close"] >= last["Low"] + 0.65 * rng
        )
        if not close_quality:
            continue

        # --- GAP / NEWS IMPULSE PROXY ---
        gap_pct = (last["Open"] - win.iloc[-1]["Close"]) / win.iloc[-1]["Close"]

        vol_hist = g.iloc[-(LOOKBACK_VOL+1):-1]["Volume"]
        vol_med = vol_hist.median()
        rvol20 = last["Volume"] / vol_med if vol_med > 0 else 0

        follow_through = last["Close"] > last["Open"] and gap_pct >= 0

        news_impulse = 0
        if gap_pct > 0.06:
            news_impulse += 1
        if rvol20 > 2.5:
            news_impulse += 1
        if close_quality:
            news_impulse += 1
        if follow_through:
            news_impulse += 1

        if news_impulse < 2:
            continue

        # --- RISK MODEL ---
        entry = float(last["Close"])
        stop  = float(last["Low"])
        r_one = entry - stop
        if r_one <= 0:
            continue

        hi250 = win["High"].max()
        reward = hi250 - entry
        rr = reward / r_one
        if rr < 2.5:
            continue

        shares = int(RISK_EUR / r_one)
        invest = shares * entry
        if shares <= 0 or invest > MAX_INVEST:
            continue

        target_2r = entry + 2 * r_one

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target_2R": round(target_2r, 2),
            "risk_per_share": round(r_one, 2),
            "shares_for_100eur": shares,
            "invest_eur": round(invest, 0),
            "green_line": round(green, 2),
            "days_below_green": int(below_green_days),
            "rr_to_250d_high": round(rr, 2),
            "rvol20": round(rvol20, 2),
            "news_impulse": news_impulse
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine CRC-REGIME Treffer.")
        return

    out.sort_values(
        by=["news_impulse", "rr_to_250d_high", "rvol20"],
        ascending=[False, False, False],
        inplace=True
    )

    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.5_CRC_REGIME_SHIFT",
            "hits": int(len(out)),
            "min_days_below_green": MIN_DAYS_BELOW_GREEN,
            "risk_model": "SL=SignalLow, TP=2R"
        }, f, indent=2)

    print(f"Done → {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()