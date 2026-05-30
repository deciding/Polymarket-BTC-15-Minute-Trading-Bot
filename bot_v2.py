import json
import os

from dotenv import load_dotenv

from v2_helpers import build_fixed_usd_buy_request
from v2_helpers import build_trade_decision
from v2_helpers import choose_trade_token


def create_market_state(market_slug: str, market_start_time: str) -> dict[str, object]:
    return {
        "market_slug": market_slug,
        "market_start_time": market_start_time,
        "quotes": [],
        "trade_taken": False,
        "winner": None,
    }


def load_runtime_config() -> dict[str, float | int]:
    load_dotenv()
    return {
        "trade_window_start_pct": float(os.getenv("TRADE_WINDOW_START_PCT", "0.6")),
        "trade_window_end_pct": float(os.getenv("TRADE_WINDOW_END_PCT", "0.8")),
        "trend_up_threshold": float(os.getenv("TREND_UP_THRESHOLD", "0.6")),
        "trend_down_threshold": float(os.getenv("TREND_DOWN_THRESHOLD", "0.4")),
        "market_buy_usd": float(os.getenv("MARKET_BUY_USD", "1.0")),
        "market_interval_seconds": int(os.getenv("MARKET_INTERVAL_SECONDS", "300")),
    }


def evaluate_market_for_paper_trade(
    market: dict[str, object],
    start_pct: float,
    end_pct: float,
    up: float,
    down: float,
    interval_seconds: int = 300,
) -> dict[str, object]:
    return build_trade_decision(
        market,
        start_pct=start_pct,
        end_pct=end_pct,
        up_threshold=up,
        down_threshold=down,
        interval_seconds=interval_seconds,
    )


def prepare_live_buy_request(
    decision: dict[str, object],
    trade_tokens: dict[str, str],
    market_buy_usd: float,
    user_usdc_balance: float,
) -> dict[str, float | str] | None:
    if not decision.get("traded"):
        return None

    token_id = choose_trade_token(trade_tokens, str(decision["side"]))
    return build_fixed_usd_buy_request(token_id, market_buy_usd, user_usdc_balance)


def main() -> dict[str, float | int]:
    config = load_runtime_config()
    print(json.dumps(config, sort_keys=True))
    return config


if __name__ == "__main__":
    main()
