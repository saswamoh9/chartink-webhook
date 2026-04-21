"""
report.py — Primary → Secondary webhook correlation report

Query params (all optional, combinable):
  days=30                  last N calendar days (default 30, max 90)
  date=YYYY-MM-DD          single IST trading day (overrides days)
  from=YYYY-MM-DD          start of IST date range (use with to=)
  to=YYYY-MM-DD            end   of IST date range (use with from=)
  primary=<slug>           filter to one primary slug
  secondary=<slug>         filter to one secondary slug
  intraday=true            only matches within 09:15–15:30 IST same session
  format=json|csv          output format (default json)

Response shape:
  summary   — overall stats, date range used, total matches
  by_link[] — per primary→secondary pair
    ├── stats             timing stats (count / avg / min / max / median minutes)
    ├── price_histogram   bucketed price_change_pct + win_rate_pct
    ├── by_date[]         matches grouped by IST calendar date (newest first)
    │     ├── date        "YYYY-MM-DD"
    │     ├── stats       per-day timing stats
    │     ├── price_histogram  per-day histogram
    │     └── matches[]  individual match records
    ├── matches[]         all matches (sorted by delta_minutes)
    ├── pending_symbols   in watchlist, never triggered secondary
    └── unmatched_signals triggered secondary without prior watchlist entry
          [{symbol, screener, fired_at_ist, trigger_price}]
"""
import csv
import io
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

IST          = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN  = (9, 15)   # 09:15 IST — NSE market open
MARKET_CLOSE = (15, 30)  # 15:30 IST — NSE market close


# ── Date / time helpers ───────────────────────────────────────────────────────

def _to_ist(dt) -> str:
    """Convert a UTC-aware datetime to a human-readable IST string."""
    if dt is None:
        return "N/A"
    try:
        return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    except Exception:
        return str(dt)


def _ist_date_str(dt) -> str:
    """Return just the IST calendar date (YYYY-MM-DD) for a UTC-aware datetime."""
    return dt.astimezone(IST).strftime("%Y-%m-%d")


def _parse_ist_date(date_str: str) -> datetime:
    """Parse 'YYYY-MM-DD' and return IST midnight (start of that day)."""
    d = datetime.strptime(date_str.strip(), "%Y-%m-%d")
    return d.replace(tzinfo=IST)


def _ist_day_bounds(date_str: str) -> tuple[datetime, datetime]:
    """Return (start_utc, end_utc) covering the full IST calendar date."""
    start = _parse_ist_date(date_str)
    end   = start.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def _resolve_time_range(
    days: int,
    date: str,
    from_date: str,
    to_date: str,
) -> tuple[datetime, datetime, str]:
    """
    Resolve query params into (start_utc, end_utc, label).
    Priority: date > from/to > days
    """
    now = datetime.now(timezone.utc)

    if date:
        start, end = _ist_day_bounds(date)
        return start, end, date

    if from_date or to_date:
        start = _parse_ist_date(from_date).astimezone(timezone.utc) if from_date else (now - timedelta(days=days))
        if to_date:
            _, end = _ist_day_bounds(to_date)
        else:
            end = now
        label = f"{from_date or '...'} → {to_date or 'today'}"
        return start, end, label

    # default: last N days
    start = now - timedelta(days=days)
    return start, now, f"last {days} days"


def _is_market_hours(dt) -> bool:
    """True if dt (UTC-aware) falls within NSE trading hours."""
    ist = dt.astimezone(IST)
    return MARKET_OPEN <= (ist.hour, ist.minute) <= MARKET_CLOSE


def _same_session(dt1, dt2) -> bool:
    """Both datetimes fall within the same NSE trading session."""
    ist1 = dt1.astimezone(IST)
    ist2 = dt2.astimezone(IST)
    return (
        ist1.date() == ist2.date()
        and _is_market_hours(dt1)
        and _is_market_hours(dt2)
    )


# ── Stats / histogram helpers ─────────────────────────────────────────────────

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


def _price_buckets(matches: list[dict]) -> dict:
    """Bucket price_change_pct and compute win_rate_pct."""
    b = {
        "< -2%":      0,
        "-2% to -1%": 0,
        "-1% to 0%":  0,
        "0% to 1%":   0,
        "1% to 2%":   0,
        "> 2%":       0,
        "N/A":        0,
    }
    for m in matches:
        pct = m.get("price_change_pct")
        if pct is None:     b["N/A"] += 1
        elif pct < -2:      b["< -2%"] += 1
        elif pct < -1:      b["-2% to -1%"] += 1
        elif pct < 0:       b["-1% to 0%"] += 1
        elif pct < 1:       b["0% to 1%"] += 1
        elif pct < 2:       b["1% to 2%"] += 1
        else:               b["> 2%"] += 1

    positive = b["0% to 1%"] + b["1% to 2%"] + b["> 2%"]
    negative = b["< -2%"] + b["-2% to -1%"] + b["-1% to 0%"]
    total    = positive + negative
    b["positive_count"] = positive
    b["negative_count"] = negative
    b["win_rate_pct"]   = round(positive / total * 100, 1) if total else None
    return b


def _group_by_date(matches: list[dict]) -> list[dict]:
    """Group matches by IST calendar date (newest first). Each day gets its own stats + histogram."""
    groups: dict[str, list[dict]] = {}
    for m in matches:
        # primary_at_ist is "YYYY-MM-DD HH:MM:SS IST" — take first 10 chars
        d = m.get("primary_at_ist", "")[:10]
        groups.setdefault(d, []).append(m)

    result = []
    for d in sorted(groups.keys(), reverse=True):   # newest first
        day_matches = groups[d]
        result.append({
            "date":            d,
            "stats":           _stats([m["delta_minutes"] for m in day_matches]),
            "price_histogram": _price_buckets(day_matches),
            "matches":         day_matches,
        })
    return result


# ── Core report builder ───────────────────────────────────────────────────────

def build_correlation_report(
    db,
    webhook_links: dict,
    days: int = 30,
    date: str = "",
    from_date: str = "",
    to_date: str = "",
    primary_filter: str = "",
    secondary_filter: str = "",
    intraday: bool = False,
) -> dict:
    """
    Build the primary→secondary correlation report.

    Time range priority: date > from_date/to_date > days
    """
    from google.cloud import firestore

    start_utc, end_utc, period_label = _resolve_time_range(days, date, from_date, to_date)

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
              .where("logged_at", ">=", start_utc)
              .where("logged_at", "<=", end_utc)
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
              .where("logged_at", ">=", start_utc)
              .where("logged_at", "<=", end_utc)
        )
        docs = [d for doc in q.stream() if (d := doc.to_dict()) and d.get("logged_at")]
        signals_by_slug[secondary_slug] = docs
        log.info(f"Report: {len(docs)} signals for slug '{secondary_slug}'")

    # ── Build per-link correlation ────────────────────────────────────────
    by_link: list[dict]     = []
    all_deltas: list[float] = []

    for primary_slug, secondary_slug in links_to_process:
        additions = additions_by_slug.get(primary_slug, [])
        signals   = signals_by_slug.get(secondary_slug, [])

        # symbol → time-sorted additions
        addition_map: dict[str, list] = {}
        for a in additions:
            addition_map.setdefault(a["symbol"], []).append(a)
        for sym in addition_map:
            addition_map[sym].sort(key=lambda x: x["logged_at"])

        matches:          list[dict] = []
        unmatched_raw:    list[dict] = []
        addition_symbols = set(addition_map.keys())
        signal_symbols   = set()

        for signal in signals:
            sym         = signal["symbol"]
            signal_time = signal["logged_at"]
            signal_symbols.add(sym)

            def _unmatched_entry():
                return {
                    "symbol":        sym,
                    "screener":      signal.get("screener", ""),
                    "fired_at_ist":  _to_ist(signal_time),
                    "trigger_price": signal.get("trigger_price", "N/A"),
                }

            if sym not in addition_map:
                unmatched_raw.append(_unmatched_entry())
                continue

            prior = [a for a in addition_map[sym] if a["logged_at"] <= signal_time]
            if not prior:
                unmatched_raw.append(_unmatched_entry())
                continue

            latest     = prior[-1]
            delta_mins = (signal_time - latest["logged_at"]).total_seconds() / 60

            if intraday and not _same_session(latest["logged_at"], signal_time):
                continue

            all_deltas.append(delta_mins)

            matches.append({
                "symbol":             sym,
                # UTC (machine use / CSV joins)
                "primary_at":         latest["logged_at"].isoformat(),
                "secondary_at":       signal_time.isoformat(),
                # IST (human readable)
                "primary_at_ist":     _to_ist(latest["logged_at"]),
                "secondary_at_ist":   _to_ist(signal_time),
                "primary_price":      latest.get("trigger_price", "N/A"),
                "secondary_price":    signal.get("trigger_price", "N/A"),
                "primary_screener":   latest.get("screener", ""),
                "secondary_screener": signal.get("screener", ""),
                "delta_minutes":      round(delta_mins, 1),
                "delta_human":        _delta_human(delta_mins),
                "price_change_pct":   _price_change(
                    latest.get("trigger_price"),
                    signal.get("trigger_price"),
                ),
            })

        matches.sort(key=lambda x: x["delta_minutes"])

        # Deduplicate unmatched by (symbol, fired_at_ist) — keep earliest per pair
        seen: set[tuple] = set()
        deduped_unmatched: list[dict] = []
        for u in sorted(unmatched_raw, key=lambda x: x["fired_at_ist"]):
            key = (u["symbol"], u["fired_at_ist"])
            if key not in seen:
                seen.add(key)
                deduped_unmatched.append(u)

        pending = sorted(addition_symbols - signal_symbols)

        by_link.append({
            "primary_slug":      primary_slug,
            "secondary_slug":    secondary_slug,
            "stats":             _stats([m["delta_minutes"] for m in matches]),
            "price_histogram":   _price_buckets(matches),
            "by_date":           _group_by_date(matches),   # ← NEW: day-wise breakdown
            "matches":           matches,
            "pending_symbols":   pending,
            "unmatched_signals": deduped_unmatched,
        })

    by_link.sort(key=lambda x: x["stats"]["count"], reverse=True)

    now_utc = datetime.now(timezone.utc)
    summary = {
        "period":           period_label,
        "period_start_ist": _to_ist(start_utc),
        "period_end_ist":   _to_ist(end_utc),
        "intraday_only":    intraday,
        "generated_at_ist": _to_ist(now_utc),
        "links_processed":  len(by_link),
        "total_matches":    sum(l["stats"]["count"] for l in by_link),
        "overall_stats":    _stats(all_deltas),
    }

    return {"summary": summary, "by_link": by_link}


# ── CSV export ────────────────────────────────────────────────────────────────

def report_to_csv(report: dict) -> str:
    """Flatten all matches across all links into a single CSV (IST columns)."""
    output = io.StringIO()
    fields = [
        "primary_slug",     "secondary_slug",
        "symbol",
        "primary_at_ist",   "primary_price",    "primary_screener",
        "secondary_at_ist", "secondary_price",   "secondary_screener",
        "delta_minutes",    "delta_human",       "price_change_pct",
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
