"""
report.py — Primary → Secondary webhook correlation report

For each defined link in WEBHOOK_LINKS, shows which stocks appeared in
the primary webhook and then later triggered the secondary webhook,
along with the time delta and price change between the two events.

Endpoint:  GET /report?days=30&primary=ema_15min_up&secondary=bullish_engulfing_...&format=json|csv
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


def _stats(deltas: list[float]) -> dict:
    if not deltas:
        return {"count": 0, "avg": None, "min": None, "max": None, "median": None}
    s = sorted(deltas)
    return {
        "count":  len(s),
        "avg":    round(sum(s) / len(s), 1),
        "min":    round(s[0], 1),
        "max":    round(s[-1], 1),
        "median": round(s[len(s) // 2], 1),
    }


# ── Core report builder ───────────────────────────────────────────────────────

def build_correlation_report(db, webhook_links: dict, days: int = 30,
                              primary_filter: str = "",
                              secondary_filter: str = "") -> dict:
    """
    For each primary → secondary link in webhook_links:
      1. Fetch all watchlist additions for the primary slug
      2. Fetch all trade signals for the secondary slug
      3. Match stocks that appear in both, signal AFTER addition
      4. Compute delta and price change

    Returns:
      summary   — overall stats across all links
      by_link   — per-link breakdown with matches
    """
    from google.cloud import firestore

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # ── Determine which links to process ─────────────────────────────────
    links_to_process: list[tuple[str, str]] = []
    for primary, secondaries in webhook_links.items():
        if primary_filter and primary != primary_filter:
            continue
        for secondary in secondaries:
            if secondary_filter and secondary != secondary_filter:
                continue
            links_to_process.append((primary, secondary))

    if not links_to_process:
        return {
            "summary": {"error": "No matching links found for the given filters"},
            "by_link": [],
        }

    # ── Fetch watchlist additions once per unique primary slug ────────────
    unique_primaries = list({p for p, _ in links_to_process})
    additions_by_slug: dict[str, list] = {}

    for primary_slug in unique_primaries:
        q = (
            db.collection("automation_results")
              .where("slug",      "==", primary_slug)
              .where("status",    "==", "added")
              .where("logged_at", ">=", cutoff)
        )
        docs = [d for doc in q.stream() if (d := doc.to_dict()) and d.get("logged_at")]
        additions_by_slug[primary_slug] = docs
        log.info(f"Report: {len(docs)} additions for slug '{primary_slug}'")

    # ── Fetch trade signals once per unique secondary slug ────────────────
    unique_secondaries = list({s for _, s in links_to_process})
    signals_by_slug: dict[str, list] = {}

    for secondary_slug in unique_secondaries:
        q = (
            db.collection("trade_signals")
              .where("slug",      "==", secondary_slug)
              .where("logged_at", ">=", cutoff)
        )
        docs = [d for doc in q.stream() if (d := doc.to_dict()) and d.get("logged_at")]
        signals_by_slug[secondary_slug] = docs
        log.info(f"Report: {len(docs)} signals for slug '{secondary_slug}'")

    # ── Build per-link correlation ────────────────────────────────────────
    by_link = []
    all_deltas: list[float] = []

    for primary_slug, secondary_slug in links_to_process:
        additions = additions_by_slug.get(primary_slug, [])
        signals   = signals_by_slug.get(secondary_slug, [])

        # Build symbol → sorted additions lookup
        addition_map: dict[str, list] = {}
        for a in additions:
            addition_map.setdefault(a["symbol"], []).append(a)
        for sym in addition_map:
            addition_map[sym].sort(key=lambda x: x["logged_at"])

        matches          = []
        unmatched_signals = []  # signal fired but no prior addition
        addition_symbols = set(addition_map.keys())
        signal_symbols   = set()

        for signal in signals:
            sym         = signal["symbol"]
            signal_time = signal["logged_at"]
            signal_symbols.add(sym)

            if sym not in addition_map:
                unmatched_signals.append(sym)
                continue

            # Most recent addition BEFORE this signal
            prior = [a for a in addition_map[sym] if a["logged_at"] <= signal_time]
            if not prior:
                unmatched_signals.append(sym)
                continue

            latest     = prior[-1]
            delta_mins = (signal_time - latest["logged_at"]).total_seconds() / 60
            all_deltas.append(delta_mins)

            matches.append({
                "symbol":           sym,
                "primary_at":       latest["logged_at"].isoformat(),
                "primary_price":    latest.get("trigger_price", "N/A"),
                "primary_screener": latest.get("screener", ""),
                "secondary_at":     signal_time.isoformat(),
                "secondary_price":  signal.get("trigger_price", "N/A"),
                "secondary_screener": signal.get("screener", ""),
                "delta_minutes":    round(delta_mins, 1),
                "delta_human":      _delta_human(delta_mins),
                "price_change_pct": _price_change(
                    latest.get("trigger_price"),
                    signal.get("trigger_price"),
                ),
            })

        matches.sort(key=lambda x: x["delta_minutes"])

        # Stocks in primary that never triggered secondary
        pending = sorted(addition_symbols - signal_symbols)

        by_link.append({
            "primary_slug":        primary_slug,
            "secondary_slug":      secondary_slug,
            "stats":               _stats([m["delta_minutes"] for m in matches]),
            "matches":             matches,
            "pending_symbols":     pending,           # in watchlist, no signal yet
            "unmatched_signals":   list(set(unmatched_signals)),  # signal without watchlist entry
        })

    # Sort links: most matches first
    by_link.sort(key=lambda x: x["stats"]["count"], reverse=True)

    # ── Overall summary ───────────────────────────────────────────────────
    summary = {
        "period_days":       days,
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "links_processed":   len(by_link),
        "total_matches":     sum(l["stats"]["count"] for l in by_link),
        "overall_stats":     _stats(all_deltas),
    }

    return {"summary": summary, "by_link": by_link}


# ── CSV export ────────────────────────────────────────────────────────────────

def report_to_csv(report: dict) -> str:
    """Flatten all matches across all links into a single CSV."""
    output = io.StringIO()
    fields = [
        "primary_slug",   "secondary_slug",
        "symbol",
        "primary_at",     "primary_price",   "primary_screener",
        "secondary_at",   "secondary_price", "secondary_screener",
        "delta_minutes",  "delta_human",     "price_change_pct",
    ]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()

    for link in report.get("by_link", []):
        for match in link.get("matches", []):
            writer.writerow({
                **match,
                "primary_slug":   link["primary_slug"],
                "secondary_slug": link["secondary_slug"],
            })

    return output.getvalue()
