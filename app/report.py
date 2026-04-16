"""
report.py — Backtesting correlation report

Joins Firestore collections:
  automation_results  → primary webhooks  (stock added to watchlist)
  trade_signals       → secondary webhooks (stock triggered a screener)

For each secondary signal, finds the most recent prior watchlist addition
of the same stock and computes the time delta between them.

Endpoint:  GET /report?days=30&symbol=TCS&format=json|csv
"""
import csv
import io
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _delta_human(minutes: float) -> str:
    if minutes < 1:
        return "<1m"
    if minutes < 60:
        return f"{int(minutes)}m"
    hours = int(minutes // 60)
    mins  = int(minutes % 60)
    return f"{hours}h {mins}m" if mins else f"{hours}h"


def _price_change(p1, p2):
    try:
        return round((float(p2) - float(p1)) / float(p1) * 100, 2)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


# ── Core report builder ───────────────────────────────────────────────────────

def build_correlation_report(db, days: int = 30, symbol_filter: str = "") -> dict:
    """
    Returns a dict with:
      summary        — aggregate stats
      correlations   — stocks that appeared in both watchlist + signals
      watchlist_only — stocks added to watchlist but no signal fired yet
      signals_only   — signals for stocks not in watchlist (entered directly)
    """
    from google.cloud import firestore

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # ── 1. Fetch watchlist additions (primary) ────────────────────────────
    q = (
        db.collection("automation_results")
          .where("status", "==", "added")
          .where("logged_at", ">=", cutoff)
    )
    if symbol_filter:
        q = q.where("symbol", "==", symbol_filter)

    additions = [d for doc in q.stream() if (d := doc.to_dict()) and d.get("logged_at")]
    log.info(f"Report: {len(additions)} watchlist additions in last {days}d")

    # ── 2. Fetch trade signals (secondary) ───────────────────────────────
    q2 = db.collection("trade_signals").where("logged_at", ">=", cutoff)
    if symbol_filter:
        q2 = q2.where("symbol", "==", symbol_filter)

    signals = [d for doc in q2.stream() if (d := doc.to_dict()) and d.get("logged_at")]
    log.info(f"Report: {len(signals)} trade signals in last {days}d")

    # ── 3. Build symbol → sorted additions lookup ─────────────────────────
    addition_map: dict[str, list] = {}
    for a in additions:
        addition_map.setdefault(a["symbol"], []).append(a)
    # Sort each symbol's additions by time (oldest first)
    for sym in addition_map:
        addition_map[sym].sort(key=lambda x: x["logged_at"])

    # ── 4. Correlate ──────────────────────────────────────────────────────
    correlations  = []
    signals_only  = []
    signal_symbols: set[str] = set()

    for signal in signals:
        sym         = signal["symbol"]
        signal_time = signal["logged_at"]
        signal_symbols.add(sym)

        if sym not in addition_map:
            signals_only.append({
                "symbol":         sym,
                "signal_at":      signal_time.isoformat(),
                "signal_price":   signal.get("trigger_price", "N/A"),
                "signal_screener": signal.get("screener", ""),
                "signal_slug":    signal.get("slug", ""),
            })
            continue

        # Most recent watchlist addition BEFORE this signal
        prior = [a for a in addition_map[sym] if a["logged_at"] <= signal_time]
        if not prior:
            signals_only.append({
                "symbol":         sym,
                "signal_at":      signal_time.isoformat(),
                "signal_price":   signal.get("trigger_price", "N/A"),
                "signal_screener": signal.get("screener", ""),
                "signal_slug":    signal.get("slug", ""),
                "note":           "signal fired before watchlist addition",
            })
            continue

        latest      = prior[-1]                                 # already sorted
        delta_mins  = (signal_time - latest["logged_at"]).total_seconds() / 60

        correlations.append({
            "symbol":             sym,
            "watchlist_added_at": latest["logged_at"].isoformat(),
            "watchlist_screener": latest.get("screener", ""),
            "watchlist_price":    latest.get("trigger_price", "N/A"),
            "signal_at":          signal_time.isoformat(),
            "signal_screener":    signal.get("screener", ""),
            "signal_slug":        signal.get("slug", ""),
            "signal_price":       signal.get("trigger_price", "N/A"),
            "delta_minutes":      round(delta_mins, 1),
            "delta_human":        _delta_human(delta_mins),
            "price_change_pct":   _price_change(
                                    latest.get("trigger_price"),
                                    signal.get("trigger_price"),
                                  ),
        })

    # Sort correlations: fastest signal first
    correlations.sort(key=lambda x: x["delta_minutes"])

    # ── 5. Watchlist-only stocks (added but no signal yet) ────────────────
    watchlist_only = []
    for sym, adds in addition_map.items():
        if sym not in signal_symbols:
            latest = adds[-1]       # most recent addition
            watchlist_only.append({
                "symbol":   sym,
                "added_at": latest["logged_at"].isoformat(),
                "price":    latest.get("trigger_price", "N/A"),
                "screener": latest.get("screener", ""),
            })
    watchlist_only.sort(key=lambda x: x["added_at"], reverse=True)

    # ── 6. Summary stats ──────────────────────────────────────────────────
    deltas  = [c["delta_minutes"] for c in correlations]
    summary = {
        "period_days":               days,
        "generated_at":              datetime.now(timezone.utc).isoformat(),
        "total_watchlist_additions": len(additions),
        "total_signals":             len(signals),
        "correlated_count":          len(correlations),
        "watchlist_only_count":      len(watchlist_only),
        "signals_only_count":        len(signals_only),
        "avg_delta_minutes":         round(sum(deltas) / len(deltas), 1) if deltas else None,
        "min_delta_minutes":         round(min(deltas), 1) if deltas else None,
        "max_delta_minutes":         round(max(deltas), 1) if deltas else None,
        "median_delta_minutes":      round(sorted(deltas)[len(deltas) // 2], 1) if deltas else None,
    }

    return {
        "summary":        summary,
        "correlations":   correlations,
        "watchlist_only": watchlist_only,
        "signals_only":   signals_only,
    }


# ── CSV export ────────────────────────────────────────────────────────────────

def report_to_csv(report: dict) -> str:
    """Serialize the correlations list to a CSV string for spreadsheet import."""
    output  = io.StringIO()
    fields  = [
        "symbol",
        "watchlist_added_at", "watchlist_price", "watchlist_screener",
        "signal_at",          "signal_price",    "signal_screener", "signal_slug",
        "delta_minutes",      "delta_human",     "price_change_pct",
    ]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(report["correlations"])
    return output.getvalue()
