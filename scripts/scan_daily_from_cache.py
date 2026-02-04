#!/usr/bin/env python3
# PAL Daily Scan – Version 1.3 (SETUP B FINAL)
#
# Fokus:
# - Range / Deckel Breakouts wie CRC
# - Kein Fib / keine grüne Linie
# - Struktur > Volumen
#
# ENTRY : Close(t0)
# STOP  : Low(t0)
#
# HARD GATES:
# - Bullische Tageskerze
# - Close in oberen 70 % der Range
# - Range-Breakout (Close > hi250)
# - Mindest-Range relativ zur 250d-Range
# - RR bis hi250-Extension >= 2.5
#
# SORTIERUNG (wichtig!):
# 1) room_to_high_r   (absteigend)
# 2) gap_strength     (absteigend)
# 3) extension_r     (aufsteigend)
# 4) rvol20           (absteigend)
# 5) score            (absteigend)

import os
import json
import pandas as pd

LOOKBACK = int(os.getenv("LOOKBACK", "250"))

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"


def today_utc_date():
    return pd.Timestamp.now("UTC").date()


def compute_score(rvol20, room_to_high_r, extension_r, close_quality, spread_est):
    s = 0.0

    s += 35.0 * min(rvol20 / 3.0, 1.0)
    s += 45.0 * min(room_to_high_r / 3.0, 1.0)

    if close_quality:
        s += 10.0

    if extension_r > 1.0:
        s -= min((extension_r - 1.0) * 20.0, 30.0)

    s -= min(spread_est * 120.0, 10.0)

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
        if len(g) < LOOKBACK + 5:
            continue

        last = g.iloc[-1]   # t0

        win = g.iloc[-(LOOKBACK + 1):-1]
        hi250 = float(win["High"].max())
        lo250 = float(win["Low"].min())
        range250 = hi250 - lo250
        if range250 <= 0:
            continue

        # --- RANGE BREAKOUT ---
        if float(last["Close"]) <= hi250:
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

        # --- BREAK STRENGTH ---
        gap_strength = (entry_px - hi250) / range250
        if gap_strength < 0.10:
            continue

        # --- RR GATE ---
        projected_high = hi250 + range250
        reward = projected_high - entry_px
        rr = reward / r_one
        if rr < 2.5:
            continue

        # --- VOLUME ---
        vol_hist = g.iloc[-21:-1]["Volume"]
        vol20_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0.0
        rvol20 = float(last["Volume"]) / vol20_med if vol20_med > 0 else 0.0
        dollar_vol = entry_px * float(last["Volume"])

        extension_r = (entry_px - hi250) / r_one
        room_to_high_r = reward / r_one
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
            "hi250": round(hi250, 2),
            "stop_low": round(stop_px, 2),
            "rr_to_target": round(rr, 2),
            "gap_strength": round(gap_strength, 3),
            "extension_r": round(extension_r, 2),
            "room_to_high_r": round(room_to_high_r, 2),
            "rvol20": round(rvol20, 2),
            "dollar_vol_today": round(dollar_vol, 0),
            "score": score,
            "setup": "B_RANGE_BREAKOUT"
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine Treffer.")
        return

    # 🔥 SETUP-B SORTIERUNG
    out.sort_values(
        ["room_to_high_r", "gap_strength", "extension_r", "rvol20", "score"],
        ascending=[False, False, True, False, False],
        inplace=True
    )

    out.insert(0, "rank", range(1, len(out) + 1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.3_setup_B_final",
            "hits": int(len(out)),
            "setup": "B_RANGE_BREAKOUT",
            "sort_logic": "room > strength > extension > volume"
        }, f, indent=2)

    print(f"Done -> {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()