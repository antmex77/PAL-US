#!/usr/bin/env python3
# PLTR Replay – v1.6 STRICT
# Ziel: Tag-für-Tag Simulation ohne Lookahead

import os
import pandas as pd
import numpy as np

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pltr_replay_v16.csv"

LOOKBACK_FIB = 250
MIN_DAYS_BELOW = 80
ATR_LOOKBACK = 14
ATR_DROP_PCT = 0.25
VOL_LOOKBACK = 20


def compute_green_line(win):
    hi = win["High"].max()
    lo = win["Low"].min()
    return hi - 0.382 * (hi - lo)


def atr(series):
    return series.mean()


def main():
    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["symbol"] == "PLTR"].sort_values("date")

    rows = []

    for i in range(LOOKBACK_FIB + MIN_DAYS_BELOW + 5, len(df)):
        hist = df.iloc[:i]
        today = df.iloc[i]

        win_fib = hist.iloc[-LOOKBACK_FIB:]
        green = compute_green_line(win_fib)

        below = hist[hist["Close"] < green]
        days_below = len(below.tail(MIN_DAYS_BELOW))

        cond_days = days_below >= MIN_DAYS_BELOW

        # --- Bearish Reset Proxy ---
        recent = hist.iloc[-60:]
        capitulation = (
            recent["Volume"].iloc[-1] > 1.8 * recent["Volume"].median()
            and recent["Close"].iloc[-1] < recent["Low"].rolling(20).min().iloc[-1] * 1.02
        )

        # --- Volatility Compression ---
        atr_now = atr(
            (recent["High"] - recent["Low"]).iloc[-ATR_LOOKBACK:]
        )
        atr_past = atr(
            (recent["High"] - recent["Low"]).iloc[-(ATR_LOOKBACK*2):-ATR_LOOKBACK]
        )
        compression = atr_now < (1 - ATR_DROP_PCT) * atr_past

        # --- Structure Shift ---
        lows = recent["Low"].rolling(5).min()
        marginal_ll = lows.iloc[-1] >= lows.iloc[-10]

        # --- Break Quality ---
        rng = today["High"] - today["Low"]
        expansion = rng > 1.3 * recent["High"].sub(recent["Low"]).median()
        vol_exp = today["Volume"] > 1.5 * recent["Volume"].median()
        break_green = today["Close"] > green

        trigger = all([
            cond_days,
            capitulation,
            compression,
            marginal_ll,
            break_green,
            expansion,
            vol_exp
        ])

        rows.append({
            "date": today["date"].date(),
            "close": round(today["Close"], 2),
            "green_line": round(green, 2),
            "days_below_green": days_below,
            "bearish_reset": capitulation,
            "compression": compression,
            "structure_shift": marginal_ll,
            "break_green": break_green,
            "range_expansion": expansion,
            "volume_expansion": vol_exp,
            "FINAL_TRIGGER": trigger
        })

    out = pd.DataFrame(rows)
    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    print(f"Replay done → {OUT_CSV}")
    print(f"Triggers found: {out['FINAL_TRIGGER'].sum()}")


if __name__ == "__main__":
    main()