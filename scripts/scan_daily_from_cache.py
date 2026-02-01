#!/usr/bin/env python3
# PAL Daily Scan – v1.3
# SETUP B ONLY: Range Acceptance Breakout (CRC-DNA)

import os
import json
import pandas as pd

LOOKBACK_250 = int(os.getenv("LOOKBACK", "250"))
RANGE_LOOKBACK = 60
MIN_REJECTIONS = 3

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"


def today_utc():
    return pd.Timestamp.utcnow().date()


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("OHLCV-Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc()]

    hits = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK_250 + RANGE_LOOKBACK + 5:
            continue

        last = g.iloc[-1]      # t0
        hist = g.iloc[:-1]     # bis t-1

        # ---- hi250 / lo250 ----
        win250 = hist.iloc[-LOOKBACK_250:]
        hi250 = float(win250["High"].max())
        lo250 = float(win250["Low"].min())
        range250 = hi250 - lo250
        if range250 <= 0:
            continue

        # ---- Range Detection ----
        range_win = hist.iloc[-RANGE_LOOKBACK:]
        range_high = float(range_win["High"].max())

        # Rejections zählen
        rej = range_win[
            (range_win["High"] >= range_high * 0.995) &
            (range_win["Close"] < range_high)
        ]
        if len(rej) < MIN_REJECTIONS:
            continue

        # ---- Break & Acceptance ----
        open_px  = float(last["Open"])
        high_px  = float(last["High"])
        low_px   = float(last["Low"])
        close_px = float(last["Close"])

        if not (
            close_px > range_high and
            low_px >= range_high and
            close_px >= open_px
        ):
            continue

        day_range = high_px - low_px
        if day_range <= 0:
            continue

        # Close-Qualität
        if close_px < (low_px + 0.60 * day_range):
            continue

        # ---- Risk / Reward ----
        entry = close_px
        stop  = range_high
        r_one = entry - stop
        if r_one <= 0:
            continue

        reward = hi250 - entry
        rr = reward / r_one
        if rr < 2.5:
            continue

        # ---- Volume ----
        vol_hist = hist.iloc[-21:]["Volume"]
        vol_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0
        rvol20 = float(last["Volume"]) / vol_med if vol_med > 0 else 0
        if rvol20 < 1.0:
            continue

        dollar_vol = entry * float(last["Volume"])
        spread_est = day_range / entry

        # ---- Score (klar & simpel) ----
        score = (
            min(rvol20 / 3.0, 1.0) * 40 +
            min(rr / 3.0, 1.0) * 40 +
            10 - min(spread_est * 100, 10)
        )
        score = round(max(0, min(100, score)), 2)

        hits.append({
            "symbol": sym,
            "date": str(last["date"]),
            "open": round(open_px, 2),
            "high": round(high_px, 2),
            "low": round(low_px, 2),
            "close": round(close_px, 2),
            "range_high": round(range_high, 2),
            "stop": round(stop, 2),
            "hi250": round(hi250, 2),
            "rr_to_hi250": round(rr, 2),
            "rvol20": round(rvol20, 2),
            "dollar_vol": round(dollar_vol, 0),
            "rejections": len(rej),
            "score": score
        })

    out = pd.DataFrame(hits)
    os.makedirs("out", exist_ok=True)

    if out.empty:
        out.to_csv(OUT_CSV, index=False)
        with open("out/summary.json", "w") as f:
            json.dump({
                "version": "v1.3_setupB",
                "hits": 0,
                "note": "Keine Range-Acceptance-Breakouts"
            }, f, indent=2)
        print("Keine Treffer.")
        return

    out.sort_values("score", ascending=False, inplace=True)
    out.insert(0, "rank", range(1, len(out) + 1))
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3_setupB",
            "hits": int(len(out)),
            "setup": "Range Acceptance Breakout",
            "min_rejections": MIN_REJECTIONS,
            "rr_gate": ">=2.5R"
        }, f, indent=2)

    print(f"Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()