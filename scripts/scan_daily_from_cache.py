#!/usr/bin/env python3
# Daily Gap Scan (Cache-only)
#
# SIGNAL (Gap-Reclaim, Variante B – sauber):
#   1) Close(t-1) <= Entry-Fib (0.382)
#   2) Open(t0)  >  Entry-Fib
#   3) Close(t0) >= Entry-Fib
#
# ENTRY: Close(t0)
# STOP : Low(t0)  (Gap-Kerze)
#
# HARD GATE:
#   RR bis hi250 >= 2.0
#
# hi250 / lo250 werden aus den letzten 250 Bars BIS t-1 berechnet (kein Look-Ahead)

import os
import json
import pandas as pd
from datetime import date

LOOKBACK = int(os.getenv("LOOKBACK", "250"))

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"


def today_utc_date():
    return pd.Timestamp.utcnow().date()


def compute_score(
    rvol20,
    room_to_high_r,
    extension_r,
    close_in_top40pct,
    spread_est
) -> float:
    """
    Score 0..100
    Volumen + Platz dominieren, Overextension wird bestraft
    """
    s = 0.0

    # Relatives Volumen (dominant)
    s += 42.0 * min(max(float(rvol20), 0.0) / 3.0, 1.0)

    # Platz bis hi250 in R
    s += 40.0 * min(max(float(room_to_high_r), 0.0) / 2.0, 1.0)

    # Close-Qualität
    if bool(close_in_top40pct):
        s += 8.0

    # Overextension-Malus
    if float(extension_r) > 1.0:
        s -= min((float(extension_r) - 1.0) * 18.0, 30.0)

    # Spread / Range-Malus
    s -= min(float(spread_est) * 100.0, 8.0)

    return round(max(0.0, min(100.0, s)), 2)


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit(f"OHLCV-Cache fehlt: {IN_CSV}")

    df = pd.read_csv(IN_CSV)
    if df.empty:
        raise SystemExit("OHLCV-Cache leer.")

    # Datum normalisieren – nur abgeschlossene Tage
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    required = ["date", "symbol", "Open", "High", "Low", "Close", "Volume"]
    for c in required:
        if c not in df.columns:
            raise SystemExit(f"Cache ohne Spalte: {c}")

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")

        # Mindestlänge
        if len(g) < LOOKBACK + 22:
            continue

        last = g.iloc[-1]   # t0
        prev = g.iloc[-2]   # t-1

        # Window für Levels (bis t-1, kein Look-Ahead)
        win = g.iloc[-(LOOKBACK + 1):-1]
        hi250 = float(win["High"].max())
        lo250 = float(win["Low"].min())

        # Fib-Level
        entry_fib = hi250 - (hi250 - lo250) * 0.382

        # --- GAP-RECLAIM REGEL ---
        gap_valid = (
            float(prev["Close"]) <= entry_fib and
            float(last["Open"])  >  entry_fib and
            float(last["Close"]) >= entry_fib
        )
        if not gap_valid:
            continue

        # ENTRY / STOP (Variante B)
        entry_px = float(last["Close"])
        stop_px  = float(last["Low"])
        r_one    = entry_px - stop_px
        if r_one <= 0:
            continue

        # RR-Gate (mind. 2R bis hi250)
        reward = hi250 - entry_px
        rr = reward / r_one if r_one > 0 else 0.0
        if rr < 2.0:
            continue

        # Volumen-Metriken
        vol_hist = g.iloc[-21:-1]["Volume"]
        vol20_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0.0
        rvol20 = (float(last["Volume"]) / vol20_med) if vol20_med > 0 else 0.0
        dollar_vol_today = float(last["Close"]) * float(last["Volume"])

        # Extension / Room in R
        extension_r = max(0.0, (entry_px - entry_fib) / r_one)
        room_to_high_r = reward / r_one

        # Close-Qualität
        rng = max(1e-6, float(last["High"]) - float(last["Low"]))
        close_in_top40pct = entry_px >= (float(last["Low"]) + 0.6 * rng)

        # Spread-Schätzung
        spread_est = rng / max(1e-6, entry_px)

        score = compute_score(
            rvol20,
            room_to_high_r,
            extension_r,
            close_in_top40pct,
            spread_est
        )

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "open": round(float(last["Open"]), 2),
            "high": round(float(last["High"]), 2),
            "low": round(float(last["Low"]), 2),
            "close": round(entry_px, 2),
            "volume": int(last["Volume"]),

            "entry_fib_0_382": round(entry_fib, 2),
            "entry_close": round(entry_px, 2),
            "stop_gap_low": round(stop_px, 2),

            "hi250": round(hi250, 2),
            "lo250": round(lo250, 2),

            "rr_to_hi250": round(rr, 2),
            "rvol20": round(rvol20, 2),
            "dollar_vol_today": round(dollar_vol_today, 2),

            "extension_r": round(extension_r, 2),
            "room_to_high_r": round(room_to_high_r, 2),
            "close_in_top40pct": bool(close_in_top40pct),
            "spread_est": round(spread_est, 4),

            "score": score
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine Treffer.")
        return

    out.sort_values(["score", "symbol"], ascending=[False, True], inplace=True)
    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    summary = {
        "bar_date": str(df["date"].max()),
        "universe": int(df["symbol"].nunique()),
        "hits": int(len(out)),
        "gap_only": True,
        "rr_gate": ">=2.0",
        "lookback": LOOKBACK
    }

    with open("out/summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()