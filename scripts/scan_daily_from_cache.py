#!/usr/bin/env python3
# PAL Daily Scan – v1.6 CRC-STRICT
#
# Ziel:
# Echte Regime-Wechsel nach bearish Reset + Akkumulation
# .382 (grüne Linie) = Gate, NICHT Signal
#
# OHLCV-only, keine externen Daten

import os
import json
import pandas as pd
import numpy as np

IN_CSV  = "data/levels_cache_250d.csv"
OUT_CSV = "out/pal_hits_daily.csv"

# ---------------- CONFIG ----------------
LOOKBACK_TOTAL      = 250
MIN_DAYS_BELOW_FIB  = 80          # steuerbar
ATR_LEN             = 14
VOL_LEN             = 20
RISK_EUR            = 100
MAX_INVEST          = 4000
MIN_RISK_PER_SHARE  = 0.5
# ----------------------------------------


def today_utc():
    return pd.Timestamp.utcnow().date()


def atr(series_h, series_l, series_c, n):
    tr = pd.concat([
        series_h - series_l,
        (series_h - series_c.shift()).abs(),
        (series_l - series_c.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def main():
    if not os.path.exists(IN_CSV):
        raise SystemExit("OHLCV Cache fehlt")

    df = pd.read_csv(IN_CSV)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] < today_utc()]

    rows = []

    for sym, g in df.groupby("symbol"):
        g = g.sort_values("date").reset_index(drop=True)
        if len(g) < LOOKBACK_TOTAL + 30:
            continue

        t0 = g.iloc[-1]
        hist = g.iloc[-(LOOKBACK_TOTAL+1):-1]

        # === GREEN LINE (.382 inverted fib, unverändert gedacht) ===
        hi = hist["High"].max()
        lo = hist["Low"].min()
        green_line = hi - 0.382 * (hi - lo)

        below = hist["Close"] < green_line
        days_below = below[::-1].cumprod().sum()

        if days_below < MIN_DAYS_BELOW_FIB:
            continue

        # === PHASE A: BEARISH RESET (min 1 von 3) ===
        reset_ok = False
        reset_type = None

        # A1 Stop-Hunt / Liquidity Sweep
        swing_low = hist["Low"].rolling(40).min().shift()
        sweep = (
            (hist["Low"] < swing_low) &
            ((hist["Close"] - hist["Low"]) /
             (hist["High"] - hist["Low"] + 1e-6) > 0.6)
        )
        if sweep.iloc[-10:].any():
            reset_ok = True
            reset_type = "liquidity_sweep"

        # A2 Volume Climax
        vol_med = hist["Volume"].rolling(VOL_LEN).median()
        climax = (
            (hist["Volume"] > 2.5 * vol_med) &
            ((hist["High"] - hist["Low"]) >
             1.5 * (hist["High"] - hist["Low"]).rolling(20).median())
        )
        if climax.iloc[-20:].any():
            reset_ok = True
            reset_type = "volume_climax"

        # A3 Failed Breakdown
        breakdown = hist["Close"] < hist["Low"].rolling(30).min().shift()
        failed = breakdown & (hist["Close"].shift(-3) > hist["Close"])
        if failed.iloc[-20:].any():
            reset_ok = True
            reset_type = "failed_breakdown"

        if not reset_ok:
            continue

        # === PHASE B: COMPRESSION / ACCUMULATION (Pflicht) ===
        atr14 = atr(hist["High"], hist["Low"], hist["Close"], ATR_LEN)
        atr_drop = atr14.iloc[-1] < 0.7 * atr14.iloc[-30]

        rng_med_now = (hist["High"] - hist["Low"]).iloc[-15:].median()
        rng_med_prev = (hist["High"] - hist["Low"]).iloc[-60:-30].median()
        range_contract = rng_med_now < 0.75 * rng_med_prev

        if not (atr_drop and range_contract):
            continue

        # === PHASE C: SUPPLY DRY / STRUCTURE SHIFT (Pflicht) ===
        down_moves = hist["Close"].diff()
        last_down = down_moves[down_moves < 0].iloc[-1]
        prev_down = down_moves[down_moves < 0].iloc[-3]

        supply_dry = abs(last_down) < 0.7 * abs(prev_down)

        marginal_lows = (
            hist["Low"].iloc[-1] >
            hist["Low"].iloc[-5] - 0.3 * atr14.iloc[-1]
        )

        if not (supply_dry or marginal_lows):
            continue

        # === PHASE D: .382 BREAK (Gate) ===
        if t0["Close"] <= green_line:
            continue

        # === PHASE E: BREAK QUALITY ===
        rng0 = t0["High"] - t0["Low"]
        rng_med = (hist["High"] - hist["Low"]).rolling(20).median().iloc[-1]
        vol_med = hist["Volume"].rolling(20).median().iloc[-1]

        if rng0 < 1.3 * rng_med:
            continue
        if t0["Volume"] < 1.5 * vol_med:
            continue
        if (t0["Close"] - t0["Low"]) / (rng0 + 1e-6) < 0.7:
            continue

        # === RISK SETUP ===
        entry = float(t0["Close"])
        stop = float(t0["Low"])
        risk_ps = entry - stop

        if risk_ps < MIN_RISK_PER_SHARE:
            continue

        shares = int(RISK_EUR / risk_ps)
        invest = shares * entry
        if shares <= 0 or invest > MAX_INVEST:
            continue

        target_2r = entry + 2 * risk_ps
        rr_to_250 = (hi - entry) / risk_ps

        rows.append({
            "symbol": sym,
            "date": str(t0["date"]),
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target_2R": round(target_2r, 2),
            "risk_per_share": round(risk_ps, 2),
            "shares_for_100eur": shares,
            "invest_eur": round(invest, 0),
            "green_line": round(green_line, 2),
            "days_below_green": int(days_below),
            "rr_to_250d_high": round(rr_to_250, 2),
            "reset_type": reset_type
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("Keine v1.6 CRC-STRICT Treffer.")
        return

    out.sort_values(
        by=["days_below_green", "rr_to_250d_high"],
        ascending=[False, False],
        inplace=True
    )
    out.insert(0, "rank", range(1, len(out)+1))

    os.makedirs("out", exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    with open("out/summary.json", "w") as f:
        json.dump({
            "version": "v1.6_CRC_STRICT",
            "hits": int(len(out)),
            "logic": "reset + compression + supply_dry + .382_gate"
        }, f, indent=2)

    print(f"Done → {OUT_CSV} | Hits={len(out)}")


if __name__ == "__main__":
    main()