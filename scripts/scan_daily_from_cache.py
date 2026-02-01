#!/usr/bin/env python3
# PAL Daily Scan – v1.3 STRUCTURAL_ACCEPT
#
# Fokus:
# - Break über klaren Range-High
# - Acceptance (kein Spike)
# - Wenige, hochwertige Trades (CRC-Typ)

import os
import json
import pandas as pd

LOOKBACK = int(os.getenv("LOOKBACK", "250"))

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"


def today_utc_date():
    return pd.Timestamp.utcnow().date()


def compute_score(rvol20, room_to_high_r, close_quality, spread_est):
    s = 0.0

    # Volumen
    s += 45.0 * min(rvol20 / 2.5, 1.0)

    # Raum
    s += 45.0 * min(room_to_high_r / 3.0, 1.0)

    # Close-Qualität
    if close_quality:
        s += 10.0

    # Spread-Malus
    s -= min(spread_est * 120.0, 15.0)

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

        last = g.iloc[-1]   # t0

        win = g.iloc[-(LOOKBACK + 1):-1]
        prior_high = float(win["High"].max())
        hi250 = prior_high
        lo250 = float(win["Low"].min())
        range250 = hi250 - lo250
        if range250 <= 0:
            continue

        entry = float(last["Close"])
        stop  = float(last["Low"])
        r_one = entry - stop
        if r_one <= 0:
            continue

        # --- STRUCTURAL BREAK ---
        if entry <= prior_high:
            continue

        # --- ACCEPTANCE ---
        rng = float(last["High"]) - float(last["Low"])
        if rng <= 0:
            continue

        close_quality = (
            entry >= float(last["Open"]) and
            entry >= (float(last["Low"]) + 0.65 * rng)
        )
        if not close_quality:
            continue

        # --- NO SPIKE ---
        if entry > prior_high + 0.5 * r_one:
            continue

        # --- LEVEL HOLD ---
        if stop < prior_high - 0.25 * r_one:
            continue

        # --- RR ---
        reward = hi250 + range250 - entry
        rr = reward / r_one
        if rr < 2.5:
            continue

        # --- VOLUME ---
        vol_hist = g.iloc[-21:-1]["Volume"]
        vol20_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0.0
        rvol20 = float(last["Volume"]) / vol20_med if vol20_med > 0 else 0.0
        if rvol20 < 1.2:
            continue

        dollar_vol = entry * float(last["Volume"])
        spread_est = rng / entry
        if spread_est > 0.08:
            continue

        room_to_high_r = reward / r_one

        score = compute_score(
            rvol20,
            room_to_high_r,
            close_quality,
            spread_est
        )

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "open": round(float(last["Open"]), 2),
            "high": round(float(last["High"]), 2),
            "low": round(float(last["Low"]), 2),
            "close": round(entry, 2),
            "prior_range_high": round(prior_high, 2),
            "stop": round(stop, 2),
            "rr_to_target": round(rr, 2),
            "rvol20": round(rvol20, 2),
            "dollar_vol_today": round(dollar_vol, 0),
            "spread_est": round(spread_est, 4),
            "score": score
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine Treffer.")
        with open("out/summary.json", "w") as f:
            json.dump({
                "version": "v1.3_structural_accept",
                "hits": 0,
                "setup": "STRUCTURAL_ACCEPT"
            }, f, indent=2)
        return

    out.sort_values(["score", "symbol"], ascending=[False, True], inplace=True)
    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3_structural_accept",
            "hits": int(len(out)),
            "setup": "STRUCTURAL_ACCEPT"
        }, f, indent=2)

    print(f"Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()