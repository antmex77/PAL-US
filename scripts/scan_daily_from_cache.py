#!/usr/bin/env python3
import pandas as pd
import json
import os
from datetime import datetime, timezone

CACHE_PATH = "data/levels_cache_250d.csv"
OUT_DIR    = "out"

OUT_ALL    = os.path.join(OUT_DIR, "pal_hits_daily.csv")
OUT_NEW    = os.path.join(OUT_DIR, "pal_hits_new.csv")
OUT_SUM    = os.path.join(OUT_DIR, "summary_daily.json")


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    if not os.path.exists(CACHE_PATH):
        raise SystemExit(f"Cache fehlt: {CACHE_PATH}")

    # -------------------------------------------------
    # Load cache
    # -------------------------------------------------
    df = pd.read_csv(CACHE_PATH, parse_dates=["date"])

    if df.empty:
        raise SystemExit("Cache ist leer.")

    # letzter Handelstag im Cache
    scan_date = df["date"].max().date()

    # -------------------------------------------------
    # === HIER kommt euer bestehender Scan ===
    # !!! WICHTIG: nichts an eurer Logik ändern !!!
    # Ergebnis MUSS DataFrame `hits` sein
    # -------------------------------------------------

    # >>>>>>>>>>>> BEISPIEL / PLATZHALTER <<<<<<<<<<<<
    # ERSETZEN durch euren echten Scan-Code
    hits = df.copy()

    # Beispiel-Filter (nur damit Script lauffähig ist)
    hits = hits[hits["Close"] > hits["Open"]]
    hits = hits.sort_values("date")
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    # -------------------------------------------------
    # Persist ALL hits
    # -------------------------------------------------
    if hits.empty:
        hits.to_csv(OUT_ALL, index=False)
    else:
        hits.to_csv(OUT_ALL, index=False)

    # -------------------------------------------------
    # NEW hits = nur scan_date
    # -------------------------------------------------
    new_hits = hits[hits["date"].dt.date == scan_date]

    if new_hits.empty:
        # leere CSV mit Header
        new_hits.head(0).to_csv(OUT_NEW, index=False)
    else:
        new_hits.to_csv(OUT_NEW, index=False)

    # -------------------------------------------------
    # Summary JSON (Single Source of Truth)
    # -------------------------------------------------
    summary = {
        "scan_timestamp_utc": utc_now(),
        "scan_date": str(scan_date),
        "cache_latest_bar": str(scan_date),
        "total_hits": int(len(hits)),
        "new_signals": int(len(new_hits)),
        "status": (
            "NO_NEW_SIGNALS"
            if len(new_hits) == 0
            else "NEW_SIGNALS"
        )
    }

    with open(OUT_SUM, "w") as f:
        json.dump(summary, f, indent=2)

    # -------------------------------------------------
    # Console Output (GitHub Actions freundlich)
    # -------------------------------------------------
    print(f"📅 Scan-Date: {scan_date}")
    print(f"📊 Total Hits: {len(hits)}")

    if len(new_hits) == 0:
        print("❌ Keine neuen Signale für diesen Handelstag.")
    else:
        print(f"✅ Neue Signale: {len(new_hits)}")

    print(f"🧾 Summary -> {OUT_SUM}")


if __name__ == "__main__":
    main()