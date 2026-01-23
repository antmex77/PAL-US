#!/usr/bin/env python3
# PAL Daily Gap Scan v1.2 (STRICT)
#
# Gap-Reclaim + Acceptance + Supply-Filter
# Ziel: nur saubere Trend-Fortsetzungen

import os
import json
import pandas as pd

LOOKBACK = int(os.getenv("LOOKBACK", "250"))

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"


def today_utc_date():
    return pd.Timestamp.utcnow().date()


def compute_score(rvol20, room_to_high_r, extension_r, close_in_top70pct, spread_est):
    s = 0.0

    # Volumen
    s += 42.0 * min(max(rvol20, 0.0) / 3.0, 1.0)

    # Platz
    s += 40.0 * min(max(room_to_high_r, 0.0) / 2.0, 1.0)

    if close_in_top70pct:
        s += 8.0

    if extension_r > 1.0:
        s -= min((extension_r - 1.0) * 18.0, 30.0)

    s -= min(spread_est * 100.0, 8.0)

    return round(max(0.0, min(100.0, s)), 2)


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("OHLCV-Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK + 30:
            continue

        last = g.iloc[-1]     # t0
        prev = g.iloc[-2]     # t-1

        win = g.iloc[-(LOOKBACK + 1):-1]
        hi250 = float(win["High"].max())
        lo250 = float(win["Low"].min())

        entry_fib = hi250 - (hi250 - lo250) * 0.382

        # --- GAP RECLAIM ---
        if not (
            float(prev["Close"]) <= entry_fib and
            float(last["Open"])  >  entry_fib and
            float(last["Close"]) >= entry_fib
        ):
            continue

        # Entry / Stop
        entry_px = float(last["Close"])
        stop_px  = float(last["Low"])
        r_one = entry_px - stop_px
        if r_one <= 0:
            continue

        # RR bis hi250
        reward = hi250 - entry_px
        rr = reward / r_one
        if rr < 2.0:
            continue

        # --- STRIKTER SUPPLY-FILTER ---
        recent_high = float(g.iloc[-21:-1]["High"].max())
        if (recent_high - entry_px) / r_one < 1.5:
            continue

        # --- GAP ACCEPTANCE ---
        if stop_px < entry_px - 0.25 * r_one:
            continue

        rng = float(last["High"] - last["Low"])
        if rng <= 0:
            continue

        close_in_top70pct = entry_px >= last["Low"] + 0.7 * rng
        if not close_in_top70pct:
            continue

        # Volumen
        vol_hist = g.iloc[-21:-1]["Volume"]
        vol20_med = vol_hist.median() if len(vol_hist) >= 10 else 0.0
        rvol20 = last["Volume"] / vol20_med if vol20_med > 0 else 0.0

        extension_r = max(0.0, (entry_px - entry_fib) / r_one)
        room_to_high_r = reward / r_one
        spread_est = rng / entry_px

        score = compute_score(
            rvol20,
            room_to_high_r,
            extension_r,
            close_in_top70pct,
            spread_est
        )

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "entry": round(entry_px, 2),
            "stop": round(stop_px, 2),
            "hi250": round(hi250, 2),
            "rr_to_hi250": round(rr, 2),
            "rvol20": round(rvol20, 2),
            "room_to_high_r": round(room_to_high_r, 2),
            "score": score
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine Treffer.")
        return

    out.sort_values("score", ascending=False, inplace=True)
    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.2-strict",
            "hits": len(out),
            "lookback": LOOKBACK
        }, f, indent=2)

    print(f"Done → {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()