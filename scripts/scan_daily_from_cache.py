#!/usr/bin/env python3
# Daily Scan (nur Cache, kein externes API)
# Signal (Gap-only, wie besprochen):
#   GAP über Entry:  Open_t0 > Entry   UND   Close_t-1 <= Entry
#
# Entry = hi250 - (hi250 - lo250)*0.382
# Stop  = hi250 - (hi250 - lo250)*0.618
#
# hi250/lo250 werden aus den letzten 250 Bars **bis t-1** berechnet (kein Look-Ahead).
# Preise auf 2 Nachkommastellen, Score 0..100 (70+ = stark).

import os, json
import pandas as pd
from datetime import date

LOOKBACK = int(os.getenv("LOOKBACK", "250"))
IN_CSV   = "data/levels_cache_250d.csv"
OUT_CSV  = "out/pal_hits_daily.csv"

def today_utc_date():
    return pd.Timestamp.utcnow().date()

def compute_score(rvol20, room_to_high_r, extension_r, close_in_top40pct, spread_est) -> float:
    s = 0.0
    # Volumen dominiert
    s += 42.0 * min(max(float(rvol20), 0.0)/3.0, 1.0)
    # Restweg bis hi250 in R
    s += 40.0 * min(max(float(room_to_high_r), 0.0)/2.0, 1.0)
    if bool(close_in_top40pct): s += 8.0
    # Malus
    if float(extension_r) > 1.0:
        s -= min((float(extension_r) - 1.0) * 18.0, 30.0)
    s -= min(float(spread_est) * 100.0, 8.0)
    return round(max(0.0, min(100.0, s)), 2)

def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit(f"OHLCV-Cache fehlt: {IN_CSV}")

    df = pd.read_csv(IN_CSV)
    if df.empty:
        raise SystemExit("OHLCV-Cache leer.")
    # Datum normalisieren & nur abgeschlossene Tage
    df["date"] = pd.to_datetime(df["date"], utc=False, errors="coerce").dt.date
    df = df[df["date"] < today_utc_date()]

    need = ["date","symbol","Open","High","Low","Close","Volume"]
    for c in need:
        if c not in df.columns:
            raise SystemExit(f"Cache ohne Spalte: {c}")

    rows = []
    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date")
        if len(g) < LOOKBACK + 22:  # 250 + mind. 22 für rvol/statistik
            continue

        last  = g.iloc[-1]     # t0
        prev  = g.iloc[-2]     # t-1
        # Window für Levels: bis t-1 (kein Look-Ahead)
        win   = g.iloc[-(LOOKBACK+1):-1]
        hi250 = float(win["High"].max())
        lo250 = float(win["Low"].min())
        entry = hi250 - (hi250 - lo250) * 0.382
        stop  = hi250 - (hi250 - lo250) * 0.618
        r_one = max(1e-6, entry - stop)

        # Gap-only Bedingung
        gap_over_entry = (float(last["Open"]) > entry) and (float(prev["Close"]) <= entry)
        if not gap_over_entry:
            continue

        # Metriken
        vol_hist = g.iloc[-21:-1]["Volume"]   # 20 Tage vor t0
        vol20_med = float(vol_hist.median()) if len(vol_hist) >= 10 else 0.0
        rvol20 = (float(last["Volume"]) / vol20_med) if vol20_med > 0 else 0.0
        dollar_vol_today = float(last["Close"]) * float(last["Volume"])

        extension_r = max(0.0, (float(last["Close"]) - entry) / r_one)
        room_to_high_r = max(0.0, (hi250 - float(last["Close"])) / r_one)

        rng = max(1e-6, float(last["High"]) - float(last["Low"]))
        close_in_top40pct = float(last["Close"]) >= (float(last["Low"]) + 0.6 * rng)
        spread_est = rng / max(1e-6, float(last["Close"]))

        score = compute_score(rvol20, room_to_high_r, extension_r, close_in_top40pct, spread_est)

        rows.append({
            "symbol": sym,
            "date": str(last["date"]),
            "open": round(float(last["Open"]), 2),
            "close": round(float(last["Close"]), 2),
            "high": round(float(last["High"]), 2),
            "low": round(float(last["Low"]), 2),
            "volume": int(last["Volume"]),
            "entry_0_382": round(entry, 2),
            "stop_0_618": round(stop, 2),
            "hi250": round(hi250, 2),
            "lo250": round(lo250, 2),
            "gap_over_entry": True,
            "gap_pct": round(((float(last["Open"]) / entry) - 1.0) * 100.0, 2),
            "rvol20": round(rvol20, 2),
            "dollar_vol_today": round(dollar_vol_today, 2),
            "extension_r": round(extension_r, 2),
            "room_to_high_r": round(room_to_high_r, 2),
            "close_in_top40pct": bool(close_in_top40pct),
            "spread_est": round(spread_est, 4),
            "score": score
        })

    out = pd.DataFrame(rows)
    out.sort_values(["score","symbol"], ascending=[False, True], inplace=True)
    out.insert(0, "rank", range(1, len(out)+1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    summary = {
        "bar_date": str(df["date"].max()),
        "universe": int(df["symbol"].nunique()),
        "hits": int(len(out)),
        "gap_only": True,
        "lookback": LOOKBACK
    }
    with open("out/summary.json","w") as f:
        f.write(json.dumps(summary, indent=2))

    print(f"Done -> {OUT_CSV} (Hits={len(out)})")

if __name__ == "__main__":
    main()
