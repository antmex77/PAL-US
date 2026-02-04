#!/usr/bin/env python3
# PAL Daily Range Breakout – Version 1.3 (Setup B)
#
# Fokus:
# - Akzeptierter Range-Breakout
# - Kein Fib / keine grüne Linie
# - Realistisches Trade-Management (2R)
#
# ENTRY : Close der Signalkerze (t0)
# STOP  : Low der Signalkerze (t0)
# TP    : 2R (rein informativ, kein Gate)

import os
import json
import pandas as pd

LOOKBACK = int(os.getenv("LOOKBACK", "250"))

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"


def today_utc_date():
    return pd.Timestamp.utcnow().date()


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("OHLCV-Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK + 5:
            continue

        last = g.iloc[-1]   # t0
        prev = g.iloc[-2]   # t-1

        win = g.iloc[-(LOOKBACK + 1):-1]
        hi250 = float(win["High"].max())
        lo250 = float(win["Low"].min())
        range250 = hi250 - lo250
        if range250 <= 0:
            continue

        # --- RANGE BREAKOUT ---
        if not (
            float(prev["Close"]) <= hi250 and
            float(last["Close"]) > hi250
        ):
            continue

        # --- ACCEPTANCE ---
        entry_px = float(last["Close"])
        open_px  = float(last["Open"])
        low_px   = float(last["Low"])
        high_px  = float(last["High"])

        rng = high_px - low_px
        if rng <= 0:
            continue

        # Close stark & oben
        if not (
            entry_px >= open_px and
            entry_px >= low_px + 0.70 * rng
        ):
            continue

        # --- R / SL / TP ---
        stop_px = low_px
        r_one = entry_px - stop_px
        if r_one <= 0:
            continue

        tp_2r = entry_px + 2.0 * r_one
        tp_3r = entry_px + 3.0 * r_one

        # --- VOLUME (Info) ---
        vol_hist = g.iloc[-21:-1]["Volume"]
        vol20_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0.0
        rvol20 = float(last["Volume"]) / vol20_med if vol20_med > 0 else 0.0
        dollar_vol = entry_px * float(last["Volume"])

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "open": round(open_px, 2),
            "high": round(high_px, 2),
            "low": round(low_px, 2),
            "close": round(entry_px, 2),

            "range_hi_250": round(hi250, 2),
            "range_lo_250": round(lo250, 2),

            "entry": round(entry_px, 2),
            "stop": round(stop_px, 2),
            "r": round(r_one, 2),
            "tp_2r": round(tp_2r, 2),
            "tp_3r": round(tp_3r, 2),

            "rvol20": round(rvol20, 2),
            "dollar_vol_today": round(dollar_vol, 0),

            "setup": "B_RANGE_BREAKOUT"
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine Treffer.")
        return

    # Sinnvolle Sortierung:
    # 1) rvol
    # 2) Dollar-Volumen
    out.sort_values(
        ["rvol20", "dollar_vol_today"],
        ascending=[False, False],
        inplace=True
    )

    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3_setup_B",
            "hits": int(len(out)),
            "entry": "close_t0",
            "stop": "low_t0",
            "tp_info": "2R / 3R",
            "fib_used": False
        }, f, indent=2)

    print(f"Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()