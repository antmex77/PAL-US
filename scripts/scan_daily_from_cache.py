#!/usr/bin/env python3
# PAL Daily Gap Scan – Version 1.2 STRICT (Signal-aware)
#
# Fokus:
# - Nur akzeptierte, dominante Gaps
# - Weniger Trades, höhere Qualität
# - Klare Aussage, ob es NEUE Signale gab
#
# SIGNAL (Gap-Reclaim, STRICT):
#   1) Close(t-1) <= EntryFib (0.382)
#   2) Open(t0)  >  EntryFib
#   3) Close(t0) >= EntryFib
#
# ENTRY: Close(t0)
# STOP : Low(t0)
#
# HARD GATES:
#   - Close >= Open
#   - Close in Top 70 % der Tagesrange
#   - Gap-Stärke >= 15 % der 250d-Range
#   - Low(t0) > EntryFib - 0.25R
#   - RR bis hi250 >= 2.5
#
# hi250 / lo250: aus letzten 250 Bars BIS t-1

import os
import json
import pandas as pd

LOOKBACK = int(os.getenv("LOOKBACK", "250"))

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"
SUMMARY = "out/summary.json"


def today_utc_date():
    return pd.Timestamp.now("UTC").date()


def compute_score(rvol20, room_to_high_r, extension_r, close_quality, spread_est):
    s = 0.0

    # Volumen (dominant)
    s += 40.0 * min(max(rvol20, 0.0) / 3.0, 1.0)

    # Platz bis hi250
    s += 40.0 * min(max(room_to_high_r, 0.0) / 2.5, 1.0)

    # Close-Qualität
    if close_quality:
        s += 10.0

    # Overextension-Malus
    if extension_r > 1.0:
        s -= min((extension_r - 1.0) * 15.0, 25.0)

    # Spread-Malus
    s -= min(spread_est * 100.0, 10.0)

    return round(max(0.0, min(100.0, s)), 2)


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("❌ OHLCV-Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    if df.empty:
        raise SystemExit("❌ OHLCV-Cache leer")

    latest_bar_date = df["date"].max()
    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK + 25:
            continue

        last = g.iloc[-1]   # t0
        prev = g.iloc[-2]   # t-1

        if last["date"] != latest_bar_date:
            continue

        win = g.iloc[-(LOOKBACK + 1):-1]
        hi250 = float(win["High"].max())
        lo250 = float(win["Low"].min())
        range250 = hi250 - lo250
        if range250 <= 0:
            continue

        entry_fib = hi250 - range250 * 0.382

        # --- GAP-RECLAIM ---
        if not (
            float(prev["Close"]) <= entry_fib and
            float(last["Open"])  >  entry_fib and
            float(last["Close"]) >= entry_fib
        ):
            continue

        entry_px = float(last["Close"])
        stop_px  = float(last["Low"])
        r_one = entry_px - stop_px
        if r_one <= 0:
            continue

        # --- ACCEPTANCE ---
        rng = float(last["High"]) - float(last["Low"])
        if rng <= 0:
            continue

        close_quality = (
            entry_px >= float(last["Open"]) and
            entry_px >= (float(last["Low"]) + 0.70 * rng)
        )
        if not close_quality:
            continue

        # --- GAP-STÄRKE ---
        gap_strength = (float(last["Open"]) - entry_fib) / range250
        if gap_strength < 0.15:
            continue

        # --- NO IMMEDIATE FILL RISK ---
        if float(last["Low"]) <= entry_fib - 0.25 * r_one:
            continue

        # --- RR-GATE ---
        reward = hi250 - entry_px
        rr = reward / r_one
        if rr < 2.5:
            continue

        # --- VOLUME ---
        vol_hist = g.iloc[-21:-1]["Volume"]
        vol20_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0.0
        rvol20 = float(last["Volume"]) / vol20_med if vol20_med > 0 else 0.0
        dollar_vol = entry_px * float(last["Volume"])

        extension_r = max(0.0, (entry_px - entry_fib) / r_one)
        room_to_high_r = rr
        spread_est = rng / entry_px

        score = compute_score(
            rvol20,
            room_to_high_r,
            extension_r,
            close_quality,
            spread_est
        )

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "open": round(float(last["Open"]), 2),
            "high": round(float(last["High"]), 2),
            "low": round(float(last["Low"]), 2),
            "close": round(entry_px, 2),
            "entry_fib_0_382": round(entry_fib, 2),
            "stop_gap_low": round(stop_px, 2),
            "hi250": round(hi250, 2),
            "rr_to_hi250": round(rr, 2),
            "gap_strength": round(gap_strength, 3),
            "rvol20": round(rvol20, 2),
            "dollar_vol_today": round(dollar_vol, 0),
            "score": score
        })

    os.makedirs("out", exist_ok=True)

    if not rows:
        # --- KEINE NEUEN SIGNALE ---
        with open(SUMMARY, "w") as f:
            json.dump({
                "version": "v1.2_strict",
                "bar_date": str(latest_bar_date),
                "status": "NO_NEW_SIGNALS",
                "new_signals": 0,
                "gap_only": True,
                "rr_gate": ">=2.5"
            }, f, indent=2)

        print("ℹ️ Keine neuen Signale für", latest_bar_date)
        return

    out = pd.DataFrame(rows)
    out.sort_values(["score", "symbol"], ascending=[False, True], inplace=True)
    out.insert(0, "rank", range(1, len(out) + 1))
    out.to_csv(OUT_CSV, index=False)

    with open(SUMMARY, "w") as f:
        json.dump({
            "version": "v1.2_strict",
            "bar_date": str(latest_bar_date),
            "status": "NEW_SIGNALS",
            "new_signals": int(len(out)),
            "gap_only": True,
            "rr_gate": ">=2.5"
        }, f, indent=2)

    print(f"✅ NEW SIGNALS: {len(out)} | Date={latest_bar_date}")


if __name__ == "__main__":
    main()