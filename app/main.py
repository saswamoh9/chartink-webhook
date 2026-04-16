import logging
import os
import signal
import threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from config import WEBHOOK_ROUTES, NOTIFICATION_CONFIG
from automation import ChartinkSession
from notify import send_notification, build_notification

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

app        = Flask(__name__)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")

_EMAIL          = None
_PASSWORD       = None
_PUSHOVER_TOKEN = None
_PUSHOVER_USER  = None
_DB             = None

# ── Background thread tracking ────────────────────────────────────────────────
_bg_threads: list[threading.Thread] = []
_bg_lock = threading.Lock()


def _spawn(target, args):
    """Start a non-daemon thread and register it for graceful drain."""
    t = threading.Thread(target=target, args=args, daemon=False)
    with _bg_lock:
        # prune finished threads while we hold the lock
        _bg_threads[:] = [x for x in _bg_threads if x.is_alive()]
        _bg_threads.append(t)
    t.start()
    return t


def _drain_threads(timeout: int = 290):
    """Wait for all background threads to finish (called on SIGTERM)."""
    with _bg_lock:
        live = [t for t in _bg_threads if t.is_alive()]
    if not live:
        return
    log.info(f"SIGTERM received — draining {len(live)} background thread(s) (max {timeout}s)...")
    for t in live:
        t.join(timeout=timeout)
    log.info("Background threads drained.")


# Gunicorn forwards SIGTERM to the worker; we drain threads before the worker exits.
signal.signal(signal.SIGTERM, lambda *_: _drain_threads())


def _route_mode(slug: str) -> str:
    cfg = NOTIFICATION_CONFIG.get(slug, {})
    watchlist = cfg.get("add_to_watchlist", False)
    notify    = cfg.get("send_notification", False)
    if watchlist and not notify:
        return "primary (watchlist only)"
    if notify and not watchlist:
        return "secondary (notify only)"
    if watchlist and notify:
        return "watchlist + notify"
    return "log only"


def _startup():
    global _EMAIL, _PASSWORD, _PUSHOVER_TOKEN, _PUSHOVER_USER, _DB
    log.info("=== Chartink Webhook Service starting ===")
    for slug, url in WEBHOOK_ROUTES.items():
        log.info(f"  /webhook/{slug} [{_route_mode(slug)}]")

    use_secrets = bool(PROJECT_ID and os.environ.get("USE_SECRET_MANAGER", "true") == "true")
    if use_secrets:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        def _get(sid):
            name = f"projects/{PROJECT_ID}/secrets/{sid}/versions/latest"
            return client.access_secret_version(name=name).payload.data.decode().strip()
        _EMAIL          = _get("chartink-email")
        _PASSWORD       = _get("chartink-password")
        _PUSHOVER_TOKEN = _get("pushover-token")
        _PUSHOVER_USER  = _get("pushover-user")
        log.info("Credentials loaded from Secret Manager.")
    else:
        _EMAIL          = os.environ["CHARTINK_EMAIL"]
        _PASSWORD       = os.environ["CHARTINK_PASSWORD"]
        _PUSHOVER_TOKEN = os.environ.get("PUSHOVER_TOKEN", "")
        _PUSHOVER_USER  = os.environ.get("PUSHOVER_USER", "")
        log.info("Credentials loaded from environment.")

    if PROJECT_ID:
        try:
            from google.cloud import firestore
            _DB = firestore.Client(project=PROJECT_ID)
            log.info("Firestore ready.")
        except Exception as e:
            log.warning(f"Firestore unavailable: {e}")


_startup()


def _parse_payload(req) -> dict:
    try:
        data = req.get_json(force=True, silent=True)
        if data and "stocks" in data:
            return data
    except Exception:
        pass
    data = req.form.to_dict()
    if data:
        return data
    try:
        import json
        return json.loads(req.data.decode("utf-8"))
    except Exception:
        pass
    return {}


def _run_automation(symbols: list, watchlist_url: str, screener: str, prices: list):
    """Stage 1 only — add stocks to watchlist via Playwright."""
    log.info(f"Automation started: {symbols} -> {watchlist_url}")
    results = []
    try:
        pairs = [(s, watchlist_url) for s in symbols]
        with ChartinkSession(_EMAIL, _PASSWORD) as session:
            results = session.process_batch(pairs)
    except Exception as e:
        log.error(f"Automation error: {e}", exc_info=True)
        results = [{"symbol": s, "watchlist": watchlist_url, "status": "error"} for s in symbols]

    summary = {r["symbol"]: r["status"] for r in results}
    log.info(f"Automation complete: {summary}")

    if _DB:
        try:
            from google.cloud import firestore
            for i, r in enumerate(results):
                _DB.collection("automation_results").add({
                    "symbol":        r["symbol"],
                    "watchlist_url": watchlist_url,
                    "screener":      screener,
                    "trigger_price": prices[i] if i < len(prices) else "N/A",
                    "status":        r["status"],
                    "logged_at":     firestore.SERVER_TIMESTAMP,
                })
        except Exception as e:
            log.warning(f"Firestore log failed: {e}")


def _log_signal(symbols: list, screener: str, prices: list, slug: str):
    """Stage 2 only — log signal to Firestore without watchlist addition."""
    if not _DB:
        return
    try:
        from google.cloud import firestore
        for i, symbol in enumerate(symbols):
            _DB.collection("trade_signals").add({
                "symbol":        symbol,
                "screener":      screener,
                "slug":          slug,
                "trigger_price": prices[i] if i < len(prices) else "N/A",
                "logged_at":     firestore.SERVER_TIMESTAMP,
            })
        log.info(f"Signal logged to Firestore: {symbols}")
    except Exception as e:
        log.warning(f"Firestore signal log failed: {e}")


@app.route("/healthz")
def health():
    with _bg_lock:
        active_threads = sum(1 for t in _bg_threads if t.is_alive())
    return jsonify({
        "status":         "ok",
        "active_threads": active_threads,
        "routes": {
            slug: {
                "mode":     _route_mode(slug),
                "watchlist": url,
            }
            for slug, url in WEBHOOK_ROUTES.items()
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/routes")
def list_routes():
    # SERVICE_URL is set by Cloud Run automatically as K_SERVICE + region, or
    # can be overridden via env var.  Falls back to the incoming request origin.
    service_url = os.environ.get("SERVICE_URL", request.host_url.rstrip("/"))
    return jsonify({
        slug: {
            "webhook_url":        f"{service_url}/webhook/{slug}",
            "watchlist_url":      url,
            "add_to_watchlist":   NOTIFICATION_CONFIG.get(slug, {}).get("add_to_watchlist", False),
            "send_notification":  NOTIFICATION_CONFIG.get(slug, {}).get("send_notification", False),
            "mode":               _route_mode(slug),
            "notification_title": NOTIFICATION_CONFIG.get(slug, {}).get("title", "N/A"),
            "symbol_description": NOTIFICATION_CONFIG.get(slug, {}).get("symbol_description", "N/A"),
        }
        for slug, url in WEBHOOK_ROUTES.items()
    })


@app.route("/webhook/<slug>", methods=["POST"])
def webhook(slug: str):
    log.info(f"Incoming [{slug}] — Content-Type: {request.content_type}")

    if slug not in WEBHOOK_ROUTES:
        return jsonify({
            "error":            f"Unknown slug '{slug}'",
            "registered_slugs": list(WEBHOOK_ROUTES.keys()),
        }), 404

    watchlist_url = WEBHOOK_ROUTES[slug]
    notif_cfg     = NOTIFICATION_CONFIG.get(slug, {})
    payload       = _parse_payload(request)

    raw     = payload.get("stocks", "")
    symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]

    if not symbols:
        return jsonify({"status": "ignored", "reason": "no symbols"}), 200

    screener = payload.get("scan_name", "Unknown")
    prices   = [p.strip() for p in payload.get("trigger_prices", "").split(",")]

    log.info(f"[{slug}] '{screener}' -> {symbols}")

    # ── Step 1: Send push notification (secondary webhooks only) ────────────
    if notif_cfg.get("send_notification", False) and _PUSHOVER_TOKEN and _PUSHOVER_USER:
        try:
            title, message = build_notification(
                slug     = slug,
                symbols  = symbols,
                prices   = prices,
                screener = screener,
                config   = notif_cfg,
            )
            log.info(f"Sending notification: '{title}'")
            send_notification(
                token    = _PUSHOVER_TOKEN,
                user     = _PUSHOVER_USER,
                title    = title,
                message  = message,
                sound    = notif_cfg.get("sound", "pushover"),
                priority = notif_cfg.get("priority", 0),
            )
        except Exception as e:
            log.warning(f"Notification error: {e}")
    else:
        log.info(f"[{slug}] Notification skipped (primary webhook — watchlist only)")

    # ── Step 2: Add to watchlist OR just log signal ───────────────────────────
    add_to_watchlist = notif_cfg.get("add_to_watchlist", False)

    if add_to_watchlist and watchlist_url:
        log.info(f"[{slug}] Stage 1: adding to watchlist {watchlist_url}")
        _spawn(_run_automation, (symbols, watchlist_url, screener, prices))
        mode = "watchlist_queued"
    else:
        log.info(f"[{slug}] Stage 2: signal only — logging to Firestore")
        _spawn(_log_signal, (symbols, screener, prices, slug))
        mode = "signal_logged"

    return jsonify({
        "status":   "accepted",
        "slug":     slug,
        "mode":     mode,
        "symbols":  symbols,
        "count":    len(symbols),
        "screener": screener,
    }), 202


@app.route("/scale-up", methods=["POST"])
def scale_up():
    log.info("Scale-up ping — service is warm.")
    return jsonify({"status": "warm"}), 200


@app.route("/scale-down", methods=["POST"])
def scale_down():
    log.info("Scale-down ping — market closed.")
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
