import logging
import urllib.request
import urllib.parse
from datetime import datetime

log = logging.getLogger(__name__)
PUSHOVER_API = "https://api.pushover.net/1/messages.json"


def send_notification(token, user, title, message, sound="pushover", priority=0):
    try:
        data = urllib.parse.urlencode({
            "token": token, "user": user, "title": title,
            "message": message, "sound": sound, "priority": priority,
        }).encode("utf-8")
        req = urllib.request.Request(PUSHOVER_API, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info(f"Pushover response: {resp.status}")
            return resp.status == 200
    except Exception as e:
        log.error(f"Pushover failed: {e}")
        return False


def build_notification(slug, symbols, prices, screener, config):
    now         = datetime.now().strftime("%I:%M %p")
    count       = len(symbols)
    description = config.get("symbol_description", "")
    footer      = config.get("footer", "")
    divider     = "\u2500" * 30
    title_line  = config.get("title_template", "{count} stock(s) at {time}").format(
                    count=count, screener=screener, time=now)
    full_title  = f"{config['title']} \u2014 {title_line}"

    stock_lines = []
    for i, symbol in enumerate(symbols):
        price = prices[i].strip() if i < len(prices) else "N/A"
        try:    price_fmt = f"\u20b9{float(price):,.2f}"
        except: price_fmt = f"\u20b9{price}"
        stock_lines.append(f"{symbol} \u2192 {price_fmt} \u2192 {description}")

    parts = [divider, "\n".join(stock_lines), divider]
    if footer:
        parts.append(footer)
    parts.append(f"Scanner: {screener}")
    return full_title, "\n".join(parts)
