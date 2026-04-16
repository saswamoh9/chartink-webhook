WEBHOOK_ROUTES: dict[str, str] = {
    # ── Primary — add symbols to watchlist (no push notification) ────────────
    "ema_15min_up":    "https://chartink.com/watchlist/1492831",
    "15_min_ema_Up":   "https://chartink.com/watchlist/1492831",   # alias used in Chartink alert URL
    "ema_15min_down":  "https://chartink.com/watchlist/REPLACE_BEARISH_ID",

    # ── Secondary — push notification only (no watchlist creation) ───────────
    "bullish_engulfing_current_close_more_previous_high_cpr_crossings": None,
    "bullish_again_crossing_15_min_ema_up":                             None,
    "bearish_engulfing_current_close_less_previous_low_cpr_crossings":  None,
    "bearish_again_crossing_15_min_ema_down":                           None,
    "macd_crossover":                                                    None,
}

NOTIFICATION_CONFIG: dict[str, dict] = {

    # ── Primary webhooks — watchlist only, no push notification ──────────────
    "ema_15min_up": {
        "add_to_watchlist":   True,
        "send_notification":  False,
        "title":              "EMA Watchlist Updated",
        "title_template":     "{count} stock(s) added at {time}",
        "symbol_description": "Added to EMA Bullish universe",
        "footer":             "Waiting for secondary signal to trade",
        "sound":              "none",
        "priority":           -1,
    },
    "15_min_ema_Up": {
        "add_to_watchlist":   True,
        "send_notification":  False,
        "title":              "EMA Watchlist Updated",
        "title_template":     "{count} stock(s) added at {time}",
        "symbol_description": "Added to EMA Bullish universe",
        "footer":             "Waiting for secondary signal to trade",
        "sound":              "none",
        "priority":           -1,
    },
    "ema_15min_down": {
        "add_to_watchlist":   True,
        "send_notification":  False,
        "title":              "EMA Watchlist Updated",
        "title_template":     "{count} stock(s) added at {time}",
        "symbol_description": "Added to EMA Bearish universe",
        "footer":             "Waiting for secondary signal to trade",
        "sound":              "none",
        "priority":           -1,
    },

    # ── Secondary webhooks — push notification only, no watchlist ─────────────
    "bullish_engulfing_current_close_more_previous_high_cpr_crossings": {
        "add_to_watchlist":   False,
        "send_notification":  True,
        "title":              "Bullish Engulfing + CPR Cross",
        "title_template":     "{count} stock(s) triggered at {time}",
        "symbol_description": "Bullish engulfing closed above prev high + CPR cross",
        "footer":             "Action: Strong bullish reversal — enter on confirmation",
        "sound":              "cashregister",
        "priority":           1,
    },
    "bullish_again_crossing_15_min_ema_up": {
        "add_to_watchlist":   False,
        "send_notification":  True,
        "title":              "Bullish EMA Re-Cross",
        "title_template":     "{count} stock(s) triggered at {time}",
        "symbol_description": "Price re-crossed 15min EMA upward — momentum resuming",
        "footer":             "Action: Pullback entry opportunity — buy on EMA support",
        "sound":              "magic",
        "priority":           1,
    },
    "bearish_engulfing_current_close_less_previous_low_cpr_crossings": {
        "add_to_watchlist":   False,
        "send_notification":  True,
        "title":              "Bearish Engulfing + CPR Cross",
        "title_template":     "{count} stock(s) triggered at {time}",
        "symbol_description": "Bearish engulfing closed below prev low + CPR cross",
        "footer":             "Action: Strong bearish reversal — short on confirmation",
        "sound":              "siren",
        "priority":           1,
    },
    "bearish_again_crossing_15_min_ema_down": {
        "add_to_watchlist":   False,
        "send_notification":  True,
        "title":              "Bearish EMA Re-Cross",
        "title_template":     "{count} stock(s) triggered at {time}",
        "symbol_description": "Price re-crossed 15min EMA downward — downtrend resuming",
        "footer":             "Action: Exit longs — short setup on EMA resistance",
        "sound":              "siren",
        "priority":           1,
    },
    "macd_crossover": {
        "add_to_watchlist":   False,
        "send_notification":  True,
        "title":              "MACD Crossover Signal",
        "title_template":     "{count} stock(s) at {time}",
        "symbol_description": "MACD crossed with EMA confirmation",
        "footer":             "Action: Enter on candle close confirmation",
        "sound":              "magic",
        "priority":           1,
    },
}
