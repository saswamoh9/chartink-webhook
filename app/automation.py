import json
import logging
import os
from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout

log = logging.getLogger(__name__)

_NSE_MAP: dict[str, str] = {}
_NSE_PATH = os.path.join(os.path.dirname(__file__), "nse.json")
try:
    with open(_NSE_PATH, "r", encoding="utf-8") as f:
        _NSE_MAP = json.load(f)
    log.info(f"Loaded {len(_NSE_MAP)} symbols from nse.json")
except Exception as e:
    log.warning(f"Could not load nse.json: {e}")


def resolve_name(symbol: str) -> str:
    name = _NSE_MAP.get(symbol.upper().strip())
    if name:
        log.info(f"  Resolved: {symbol} -> '{name}'")
        return name
    log.warning(f"  '{symbol}' not in nse.json, using symbol directly")
    return symbol


class ChartinkSession:
    LOGIN_URL = "https://chartink.com/login"

    def __init__(self, email: str, password: str):
        self.email                    = email
        self.password                 = password
        self._pw                      = None
        self._browser                 = None
        self._ctx                     = None
        self._page: Page | None       = None
        self._current_url: str | None = None

    def start(self):
        self._pw      = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        self._ctx = self._browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        self._page = self._ctx.new_page()
        log.info("Browser started.")

    def close(self):
        try:
            if self._ctx:     self._ctx.close()
            if self._browser: self._browser.close()
            if self._pw:      self._pw.stop()
        except Exception:
            pass
        log.info("Browser closed.")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.close()

    def _do_login(self) -> bool:
        p = self._page
        log.info("Login page — filling credentials...")
        p.wait_for_selector("#login-email", timeout=15000)
        p.fill("#login-email", self.email)
        p.fill("#login-password", self.password)
        p.wait_for_timeout(500)
        p.locator("button:has-text('Log in')").first.click(timeout=10000)
        log.info("Clicked Log in")
        p.wait_for_function(
            "() => !window.location.href.includes('/login')",
            timeout=30000,
        )
        p.wait_for_timeout(2000)
        log.info(f"Login complete. URL: {p.url}")
        return True

    def open_watchlist(self, watchlist_url: str) -> bool:
        p = self._page
        if self._current_url == watchlist_url and "watchlist" in p.url:
            log.info(f"Already on watchlist: {watchlist_url}")
            return True
        log.info(f"Navigating to: {watchlist_url}")
        p.goto(watchlist_url, wait_until="domcontentloaded", timeout=60000)
        p.wait_for_timeout(3000)
        log.info(f"Landed on: {p.url}")
        if "login" in p.url:
            if not self._do_login():
                return False
            log.info(f"Going back to watchlist: {watchlist_url}")
            p.goto(watchlist_url, wait_until="domcontentloaded", timeout=60000)
            p.wait_for_timeout(3000)
            log.info(f"Now on: {p.url}")
        if "watchlist" in p.url:
            self._current_url = watchlist_url
            log.info("Watchlist page ready.")
            return True
        log.error(f"Could not open watchlist. URL: {p.url}")
        return False

    def _clear_search(self, search):
        """Reliably clear the search input.
        Uses fill('') instead of click+keypress — more resilient when the
        page is mid-update (e.g. right after adding a stock)."""
        try:
            search.wait_for(state="visible", timeout=10000)
            search.fill("")
            self._page.wait_for_timeout(300)
        except Exception:
            self._page.wait_for_timeout(1000)  # let page settle, then move on

    def add_stock(self, symbol: str) -> str:
        p            = self._page
        company_name = resolve_name(symbol)

        try:
            search = p.locator("#search").first
            search.wait_for(state="visible", timeout=15000)

            # Try company name first, then raw symbol as fallback
            terms = [company_name, symbol] if company_name != symbol else [symbol]

            for term in terms:
                log.info(f"  Searching: '{term}'")

                # ── Clear & fill (fill is instant — no per-char delay) ──────────
                self._clear_search(search)
                search.fill(term)
                p.wait_for_timeout(500)          # wait for debounce

                # ── Wait for dropdown to actually appear (up to 6s) ────────────
                try:
                    p.wait_for_selector(
                        "div.watchlist span",
                        state="visible",
                        timeout=6000,
                    )
                except PWTimeout:
                    log.warning(f"  Dropdown didn't appear for '{term}' — trying next term")
                    self._clear_search(search)
                    continue

                dropdown = p.locator("div.watchlist span")
                count    = dropdown.count()
                log.info(f"  Dropdown items: {count}")

                if count == 0:
                    log.warning(f"  Empty dropdown for '{term}'")
                    self._clear_search(search)
                    continue

                # ── Prefer exact company-name match ───────────────────────────
                for i in range(count):
                    try:
                        text = dropdown.nth(i).inner_text().strip()
                        if text.lower() == company_name.lower():
                            log.info(f"  Exact match [{i}]: '{text}' — clicking")
                            dropdown.nth(i).click()
                            p.wait_for_timeout(2000)     # let page process the add
                            log.info(f"  {symbol} -> added (exact match)")
                            return "added"
                    except Exception:
                        continue

                # ── Fall back: click first non-empty suggestion ───────────────
                for i in range(count):
                    try:
                        text = dropdown.nth(i).inner_text().strip()
                        log.info(f"    [{i}] '{text}'")
                        if text and "<!--" not in text:
                            log.info(f"  First match [{i}]: '{text}' — clicking")
                            dropdown.nth(i).click()
                            p.wait_for_timeout(2000)     # let page process the add
                            log.info(f"  {symbol} -> added")
                            return "added"
                    except Exception:
                        continue

            log.warning(f"  {symbol} -> not_found (exhausted all terms)")
            return "not_found"

        except PWTimeout:
            log.warning(f"  {symbol} -> timeout")
            return "timeout"
        except Exception as e:
            log.error(f"  {symbol} -> error: {e}", exc_info=True)
            return "error"

    def process_batch(self, items: list[tuple[str, str]]) -> list[dict]:
        """Process all (symbol, watchlist_url) pairs.
        Navigates to each watchlist only once — reuses the open page for
        consecutive stocks going to the same watchlist."""
        results = []
        for symbol, watchlist_url in items:
            if not self.open_watchlist(watchlist_url):
                results.append({
                    "symbol":   symbol,
                    "watchlist": watchlist_url,
                    "status":   "watchlist_error",
                })
                continue
            status = self.add_stock(symbol)
            results.append({
                "symbol":   symbol,
                "watchlist": watchlist_url,
                "status":   status,
            })
            log.info(f"  [{symbol}] => {status}")
        return results
