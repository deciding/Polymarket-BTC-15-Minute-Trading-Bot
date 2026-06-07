import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _validate_interval_seconds(interval_seconds: int) -> None:
    if (
        not isinstance(interval_seconds, int)
        or interval_seconds <= 0
        or interval_seconds % 60 != 0
    ):
        raise ValueError("interval_seconds must be a positive whole-minute value")


def _validate_count(count: int) -> None:
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise ValueError("count must be a non-negative integer")


def _to_utc_timestamp(now: datetime) -> int:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return int(now.astimezone(timezone.utc).timestamp())


def get_current_btc_market_slug(now: datetime, interval_seconds: int = 300) -> str:
    _validate_interval_seconds(interval_seconds)
    interval_start = (_to_utc_timestamp(now) // interval_seconds) * interval_seconds
    interval_min = interval_seconds // 60
    return f"btc-updown-{interval_min}m-{interval_start}"


def get_next_btc_market_slugs(
    now: datetime, count: int, interval_seconds: int = 300
) -> list[str]:
    """Return slugs for the current interval followed by the next intervals."""
    _validate_interval_seconds(interval_seconds)
    _validate_count(count)
    interval_start = (_to_utc_timestamp(now) // interval_seconds) * interval_seconds
    interval_min = interval_seconds // 60
    return [
        f"btc-updown-{interval_min}m-{interval_start + i * interval_seconds}"
        for i in range(count)
    ]


def should_sample_quote(
    last_sample_ts: float | None, now_ts: float, sample_interval: float = 3.0
) -> bool:
    if last_sample_ts is None:
        return True
    return (now_ts - last_sample_ts) >= sample_interval


def format_collateral_balance(balance: dict[str, Any]) -> str:
    return f"USDC total: ${float(balance['human']):.2f}"


def format_deposit_wallet_status(wallet: str, deployed: bool) -> str:
    return f"deposit_wallet={wallet} deployed={deployed}"


def build_fixed_usd_buy_request(
    token_id: str, amount: float, user_usdc_balance: float
) -> dict[str, float | str]:
    return {
        "token_id": token_id,
        "amount": amount,
        "user_usdc_balance": user_usdc_balance,
    }


def choose_trade_token(trade_tokens: dict[str, str], side: str) -> str:
    return trade_tokens[side]


def _normalize_outcome(value: str) -> str:
    outcome = value.strip().upper()
    if outcome in {"YES", "UP"}:
        return "UP"
    if outcome in {"NO", "DOWN"}:
        return "DOWN"
    return outcome


def _parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return ts


def select_gamma_market(markets: list[dict[str, Any]], slug: str) -> dict[str, Any] | None:
    for market in markets:
        if market.get("slug") == slug:
            return market
    return None


def extract_market_winner(market: dict[str, Any]) -> str:
    resolution = market.get("resolution")
    if isinstance(resolution, str) and resolution.strip():
        return _normalize_outcome(resolution)

    tokens = market.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if token.get("winner") is True:
                outcome = token.get("outcome")
                if isinstance(outcome, str) and outcome.strip():
                    return _normalize_outcome(outcome)

    outcomes_raw = market.get("outcomes")
    prices_raw = market.get("outcomePrices")
    if isinstance(outcomes_raw, str) and isinstance(prices_raw, str):
        try:
            outcomes = json.loads(outcomes_raw)
            prices = json.loads(prices_raw)
        except (ValueError, TypeError, json.JSONDecodeError):
            return "UNKNOWN"

        for outcome, price in zip(outcomes, prices):
            try:
                numeric_price = float(price)
            except (TypeError, ValueError):
                return "UNKNOWN"
            if numeric_price >= 0.999:
                return _normalize_outcome(str(outcome))

    return "UNKNOWN"


def build_trade_decision(
    market: dict[str, Any],
    start_pct: float,
    end_pct: float,
    up_threshold: float,
    down_threshold: float,
    interval_seconds: int = 300,
) -> dict[str, Any]:
    _validate_interval_seconds(interval_seconds)
    start_time = _parse_ts(str(market["market_start_time"]))
    window_start = start_pct * interval_seconds
    window_end = end_pct * interval_seconds

    for quote in market.get("quotes", []):
        quote_time = _parse_ts(str(quote["ts"]))
        elapsed = (quote_time - start_time).total_seconds()
        if not (window_start <= elapsed < window_end):
            continue

        bid = float(quote["bid"])
        ask = float(quote["ask"])
        mid_price = (bid + ask) / 2.0

        if ask > up_threshold:
            payout = 1.0 if market.get("winner") == "UP" else 0.0
            return {
                "traded": True,
                "side": "UP",
                "entry_price": ask,
                "spent": mid_price,
                "payout": payout,
                "profit": payout - mid_price,
            }

        if bid < down_threshold:
            spent = 1.0 - mid_price
            payout = 1.0 if market.get("winner") == "DOWN" else 0.0
            return {
                "traded": True,
                "side": "DOWN",
                "entry_price": bid,
                "spent": spent,
                "payout": payout,
                "profit": payout - spent,
            }

    return {
        "traded": False,
        "side": None,
        "entry_price": None,
        "spent": 0.0,
        "payout": 0.0,
        "profit": 0.0,
    }


def build_trade_decision_dual(
    market: dict[str, Any],
    start_pct: float,
    end_pct: float,
    up_threshold: float,
    down_threshold: float,
    interval_seconds: int = 300,
) -> dict[str, Any]:
    _validate_interval_seconds(interval_seconds)
    start_time = _parse_ts(str(market["market_start_time"]))
    window_start = start_pct * interval_seconds
    window_end = end_pct * interval_seconds

    for quote in market.get("quotes", []):
        elapsed = (_parse_ts(str(quote["ts"])) - start_time).total_seconds()
        if not (window_start <= elapsed < window_end):
            continue

        up_ask = float(quote["up_ask"])
        down_ask = float(quote["down_ask"])

        if up_ask > up_threshold:
            payout = 1.0 if market.get("winner") == "UP" else 0.0
            return {
                "traded": True,
                "side": "UP",
                "entry_price": up_ask,
                "spent": up_ask,
                "payout": payout,
                "profit": payout - up_ask,
            }

        if down_ask > up_threshold:
            payout = 1.0 if market.get("winner") == "DOWN" else 0.0
            return {
                "traded": True,
                "side": "DOWN",
                "entry_price": down_ask,
                "spent": down_ask,
                "payout": payout,
                "profit": payout - down_ask,
            }

    return {
        "traded": False,
        "side": None,
        "entry_price": None,
        "spent": 0.0,
        "payout": 0.0,
        "profit": 0.0,
    }


def evaluate_market_trade(
    market: dict[str, Any],
    start_pct: float,
    end_pct: float,
    up_threshold: float,
    down_threshold: float,
    interval_seconds: int = 300,
) -> dict[str, Any]:
    return build_trade_decision(
        market,
        start_pct=start_pct,
        end_pct=end_pct,
        up_threshold=up_threshold,
        down_threshold=down_threshold,
        interval_seconds=interval_seconds,
    )


def append_market_tape_block(path: str | Path, block: dict[str, Any]) -> None:
    with Path(path).open("a") as f:
        f.write(json.dumps(block) + "\n")
        f.flush()


def append_close_market_log(path: str | Path, summary: dict[str, Any]) -> None:
    with Path(path).open("a") as f:
        f.write(f"[{summary['timestamp']}] Market: {summary['market_slug']}\n")
        f.write(f"  UP: {float(summary['up_shares']):.4f} shares | ${float(summary['up_usd']):.2f}\n")
        f.write(f"  DOWN: {float(summary['down_shares']):.4f} shares | ${float(summary['down_usd']):.2f}\n")
        f.write(f"  Winner: {summary['winner']}\n")
        f.write("\n")
        f.flush()
