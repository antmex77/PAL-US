#!/usr/bin/env python3
# PAL Daily Gap Scan – Version 1.3 (SETUP B / CRC ONLY)
#
# Ziel:
#   Ausschließlich CRC-ähnliche Trades:
#   - Mehrfach getestete Range
#   - Saubere Acceptance über Range-High
#   - Expansion + Volumen
#
# SETUP B – Range Acceptance Breakout
#
# SIGNAL:
#   1) Mehrere Rejections an Range-High (>=2)
#   2) Close(t0) > Range-High
#   3) Close in oberem 60 % der Tagesrange
#
# ENTRY: Close(t0)
# STOP : Range-High
#
# HARD GATES:
#   - RR bis hi250 >= 2.5
#   - rVol20 >= 1.2
#   - Spread < 6 %
#
# hi250 / lo250: letzte 250 Bars bis t-1

import os
import json
import pandas as pd

LOOKBACK = int(os.getenv("LOOKBACK", "250"))
IN_CSV   = "data/levels_cache_250d.csv"
OUT_CSV  = "out/pal_hits_daily.csv"


def today_utc_date():
    return pd.Timestamp.now("UTC").date()


def compute_score(rvol20, room_r, close_quality, spread):
    s = 0.0

    s += 45.0 * min(rvol20 / 3.0, 1.0)
    s += 45.0 * min(room_r / 3.0, 1.0)

    if close_quality:
        s += 10.0

    s -= min(spread * 100.0, 10.0)

    return round(max(0.0, min(100.0, s)), 2)


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("levels_cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK + 30:
            continue

        last = g.iloc[-1]   # t0
        prev = g.iloc[-2]

        win = g.iloc[-(LOOKBACK + 1):-1]
        hi250 = float(win["High"].max())
        lo250 = float(win["Low"].min())
        range250 = hi250 - lo250
        if range250 <= 0:
            continue

        # --- RANGE HIGH (20d) ---
        range_win = g.iloc[-21:-1]
        range_high = float(range_win["High"].max())

        # --- REJECTIONS ---
        rejections = (range_win["High"] >= range_high * 0.995).sum()
        if rejections < 2:
            continue

        # --- BREAK & ACCEPT ---
        entry = float(last["Close"])
        if entry <= range_high:
            continue

        rng = float(last["High"]) - float(last["Low"])
        if rng <= 0:
            continue

        close_quality = entry >= float(last["Low"]) + 0.60 * rng
        if not close_quality:
            continue

        stop = range_high
        r_one = entry - stop
        if r_one <= 0:
            continue

        reward = hi250 - entry
        rr = reward / r_one
        if rr < 2.5:
            continue

        # --- VOLUME ---
        vol_hist = g.iloc[-21:-1]["Volume"]
        vol_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0.0
        rvol20 = float(last["Volume"]) / vol_med if vol_med > 0 else 0.0
        if rvol20 < 1.2:
            continue

        spread = rng / entry
        if spread > 0.06:
            continue

        score = compute_score(
            rvol20,
            rr,
            close_quality,
            spread
        )

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "open": round(float(last["Open"]), 2),
            "high": round(float(last["High"]), 2),
            "low": round(float(last["Low"]), 2),
            "close": round(entry, 2),
            "range_high": round(range_high, 2),
            "stop": round(stop, 2),
            "hi250": round(hi250, 2),
            "rr_to_hi250": round(rr, 2),
            "rvol20": round(rvol20, 2),
            "dollar_vol": round(entry * float(last["Volume"]), 0),
            "rejections": int(rejections),
            "score": score
        })

    os.makedirs("out", exist_ok=True)

    if not rows:
        empty = pd.DataFrame(columns=[
            "rank","symbol","date","open","high","low","close",
            "range_high","stop","hi250","rr_to_hi250",
            "rvol20","dollar_vol","rejections","score"
        ])
        empty.to_csv(OUT_CSV, index=False)

        with open("out/summary.json", "w") as f:
            json.dump({
                "version": "v1.3_setupB",
                "hits": 0,
                "note": "No CRC-style range acceptance breakouts"
            }, f, indent=2)

        print("Keine Treffer.")
        return

    out = pd.DataFrame(rows)
    out.sort_values(["score", "symbol"], ascending=[False, True], inplace=True)
    out.insert(0, "rank", range(1, len(out) + 1))
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3_setupB",
            "hits": int(len(out)),
            "setup": "CRC Range Acceptance Breakout"
        }, f, indent=2)

    print(f"Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()