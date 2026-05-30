import json
from datetime import datetime, timezone

import pytest

from bot_v2 import create_market_state
from bot_v2 import evaluate_market_for_paper_trade
from bot_v2 import load_runtime_config
from bot_v2 import prepare_live_buy_request
from v2_helpers import build_trade_decision
from v2_helpers import extract_market_winner
from v2_helpers import evaluate_market_trade
from v2_helpers import append_close_market_log
from v2_helpers import append_market_tape_block
from v2_helpers import build_fixed_usd_buy_request
from v2_helpers import choose_trade_token
from v2_helpers import format_collateral_balance
from v2_helpers import format_deposit_wallet_status
from v2_helpers import get_current_btc_market_slug
from v2_helpers import get_next_btc_market_slugs
from v2_helpers import select_gamma_market
from v2_helpers import should_sample_quote


def test_get_current_btc_market_slug_floors_to_current_5m_window() -> None:
    now = datetime(2026, 5, 30, 7, 52, 34, tzinfo=timezone.utc)
    assert get_current_btc_market_slug(now, 300) == "btc-updown-5m-1780127400"


def test_create_market_state_initializes_per_market_runtime_state() -> None:
    market_start_time = "2026-05-17T14:40:00+00:00"

    assert create_market_state("btc-updown-5m-1780127400", market_start_time) == {
        "market_slug": "btc-updown-5m-1780127400",
        "market_start_time": market_start_time,
        "quotes": [],
        "trade_taken": False,
        "winner": None,
    }


def test_evaluate_market_for_paper_trade_smoke_returns_traded_true() -> None:
    market = {
        "market_start_time": "2026-05-17T14:40:00+00:00",
        "winner": "UP",
        "quotes": [
            {"ts": "2026-05-17T14:42:30+00:00", "bid": 0.61, "ask": 0.62},
        ],
    }

    result = evaluate_market_for_paper_trade(
        market,
        start_pct=0.50,
        end_pct=0.80,
        up=0.60,
        down=0.40,
    )

    assert result["traded"] is True


def test_build_trade_decision_returns_traded_true_for_qualifying_quote() -> None:
    market = {
        "market_start_time": "2026-05-17T14:40:00+00:00",
        "winner": "UP",
        "quotes": [
            {"ts": "2026-05-17T14:42:30+00:00", "bid": 0.61, "ask": 0.62},
        ],
    }

    result = build_trade_decision(
        market,
        start_pct=0.50,
        end_pct=0.80,
        up_threshold=0.60,
        down_threshold=0.40,
    )

    assert result["traded"] is True


def test_get_current_btc_market_slug_preserves_exact_5m_boundary() -> None:
    now = datetime(2026, 5, 30, 7, 50, 0, tzinfo=timezone.utc)
    assert get_current_btc_market_slug(now, 300) == "btc-updown-5m-1780127400"


def test_get_current_btc_market_slug_rejects_invalid_interval_seconds() -> None:
    now = datetime(2026, 5, 30, 7, 50, 0, tzinfo=timezone.utc)

    with pytest.raises(
        ValueError, match="interval_seconds must be a positive whole-minute value"
    ):
        get_current_btc_market_slug(now, 0)


def test_get_current_btc_market_slug_rejects_non_minute_interval() -> None:
    now = datetime(2026, 5, 30, 7, 50, 0, tzinfo=timezone.utc)

    with pytest.raises(
        ValueError, match="interval_seconds must be a positive whole-minute value"
    ):
        get_current_btc_market_slug(now, 90)


def test_get_current_btc_market_slug_rejects_naive_datetime() -> None:
    now = datetime(2026, 5, 30, 7, 50, 0)

    with pytest.raises(ValueError, match="now must be timezone-aware"):
        get_current_btc_market_slug(now, 300)


def test_get_next_btc_market_slugs_returns_sequence() -> None:
    now = datetime(2026, 5, 30, 7, 52, 34, tzinfo=timezone.utc)
    assert get_next_btc_market_slugs(now, 3, 300) == [
        "btc-updown-5m-1780127400",
        "btc-updown-5m-1780127700",
        "btc-updown-5m-1780128000",
    ]


def test_get_next_btc_market_slugs_rejects_invalid_interval_seconds() -> None:
    now = datetime(2026, 5, 30, 7, 52, 34, tzinfo=timezone.utc)

    with pytest.raises(
        ValueError, match="interval_seconds must be a positive whole-minute value"
    ):
        get_next_btc_market_slugs(now, 3, -300)


def test_get_next_btc_market_slugs_rejects_invalid_count() -> None:
    now = datetime(2026, 5, 30, 7, 52, 34, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="count must be a non-negative integer"):
        get_next_btc_market_slugs(now, -1, 300)


def test_get_next_btc_market_slugs_rejects_non_integer_count() -> None:
    now = datetime(2026, 5, 30, 7, 52, 34, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="count must be a non-negative integer"):
        get_next_btc_market_slugs(now, 1.5, 300)


def test_get_next_btc_market_slugs_rejects_bool_count() -> None:
    now = datetime(2026, 5, 30, 7, 52, 34, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="count must be a non-negative integer"):
        get_next_btc_market_slugs(now, True, 300)


def test_should_sample_quote_every_3_seconds() -> None:
    assert should_sample_quote(None, 100.0, 3.0) is True
    assert should_sample_quote(100.0, 102.9, 3.0) is False
    assert should_sample_quote(100.0, 103.0, 3.0) is True


def test_select_gamma_market_returns_matching_slug() -> None:
    markets = [
        {"slug": "other-market", "question": "other"},
        {"slug": "btc-updown-5m-1780127400", "question": "target"},
    ]

    assert select_gamma_market(markets, "btc-updown-5m-1780127400") == {
        "slug": "btc-updown-5m-1780127400",
        "question": "target",
    }


def test_select_gamma_market_returns_none_when_slug_missing() -> None:
    markets = [{"slug": "other-market", "question": "other"}]

    assert select_gamma_market(markets, "btc-updown-5m-1780127400") is None


def test_extract_market_winner_uses_outcome_prices_when_price_hits_one() -> None:
    market = {
        "outcomes": json.dumps(["Up", "Down"]),
        "outcomePrices": json.dumps(["1.0", "0.0"]),
    }

    assert extract_market_winner(market) == "UP"


def test_extract_market_winner_uses_resolution_field_first() -> None:
    market = {
        "resolution": "Down",
        "outcomes": json.dumps(["Up", "Down"]),
        "outcomePrices": json.dumps(["1.0", "0.0"]),
    }

    assert extract_market_winner(market) == "DOWN"


def test_extract_market_winner_uses_token_winner_flags() -> None:
    market = {
        "tokens": [
            {"outcome": "Up", "winner": True},
            {"outcome": "Down", "winner": False},
        ]
    }

    assert extract_market_winner(market) == "UP"


def test_extract_market_winner_accepts_near_one_prices() -> None:
    market = {
        "outcomes": json.dumps(["Up", "Down"]),
        "outcomePrices": json.dumps(["0.999", "0.001"]),
    }

    assert extract_market_winner(market) == "UP"


def test_extract_market_winner_returns_unknown_for_malformed_numeric_price() -> None:
    market = {
        "outcomes": json.dumps(["Up", "Down"]),
        "outcomePrices": json.dumps(["abc", "0.0"]),
    }

    assert extract_market_winner(market) == "UNKNOWN"


def test_extract_market_winner_returns_unknown_for_malformed_json() -> None:
    market = {
        "outcomes": "[not valid json",
        "outcomePrices": json.dumps(["1.0", "0.0"]),
    }

    assert extract_market_winner(market) == "UNKNOWN"


def test_evaluate_market_trade_takes_first_qualifying_trade_inside_window() -> None:
    market = {
        "market_start_time": "2026-05-17T14:40:00+00:00",
        "winner": "UP",
        "quotes": [
            {"ts": "2026-05-17T14:42:29+00:00", "bid": 0.59, "ask": 0.61},
            {"ts": "2026-05-17T14:42:30+00:00", "bid": 0.61, "ask": 0.62},
            {"ts": "2026-05-17T14:42:45+00:00", "bid": 0.80, "ask": 0.81},
            {"ts": "2026-05-17T14:44:00+00:00", "bid": 0.20, "ask": 0.21},
        ],
    }

    result = evaluate_market_trade(
        market,
        start_pct=0.50,
        end_pct=0.80,
        up_threshold=0.60,
        down_threshold=0.40,
    )

    assert result == {
        "traded": True,
        "side": "UP",
        "entry_price": 0.62,
        "spent": 0.615,
        "payout": 1.0,
        "profit": 0.385,
    }


def test_evaluate_market_trade_prices_down_side_from_one_minus_mid() -> None:
    market = {
        "market_start_time": "2026-05-17T14:40:00+00:00",
        "winner": "UP",
        "quotes": [
            {"ts": "2026-05-17T14:43:00+00:00", "bid": 0.02, "ask": 0.03},
        ],
    }

    result = evaluate_market_trade(
        market,
        start_pct=0.50,
        end_pct=0.80,
        up_threshold=0.60,
        down_threshold=0.40,
    )

    assert result == {
        "traded": True,
        "side": "DOWN",
        "entry_price": 0.02,
        "spent": 0.975,
        "payout": 0.0,
        "profit": -0.975,
    }


def test_evaluate_market_trade_skips_quotes_at_end_boundary_and_returns_no_trade() -> None:
    market = {
        "market_start_time": "2026-05-17T14:40:00+00:00",
        "winner": "DOWN",
        "quotes": [
            {"ts": "2026-05-17T14:44:00+00:00", "bid": 0.10, "ask": 0.11},
        ],
    }

    result = evaluate_market_trade(
        market,
        start_pct=0.50,
        end_pct=0.80,
        up_threshold=0.60,
        down_threshold=0.40,
    )

    assert result == {
        "traded": False,
        "side": None,
        "entry_price": None,
        "spent": 0.0,
        "payout": 0.0,
        "profit": 0.0,
    }


def test_evaluate_market_trade_rejects_naive_market_timestamp() -> None:
    market = {
        "market_start_time": "2026-05-17T14:40:00",
        "winner": "UP",
        "quotes": [
            {"ts": "2026-05-17T14:42:30+00:00", "bid": 0.61, "ask": 0.62},
        ],
    }

    with pytest.raises(ValueError, match="timestamp must be timezone-aware"):
        evaluate_market_trade(
            market,
            start_pct=0.50,
            end_pct=0.80,
            up_threshold=0.60,
            down_threshold=0.40,
        )


def test_evaluate_market_trade_uses_supplied_15_minute_interval_for_window() -> None:
    market = {
        "market_start_time": "2026-05-17T14:30:00+00:00",
        "winner": "UP",
        "quotes": [
            {"ts": "2026-05-17T14:37:00+00:00", "bid": 0.59, "ask": 0.61},
            {"ts": "2026-05-17T14:38:00+00:00", "bid": 0.61, "ask": 0.62},
        ],
    }

    result = evaluate_market_trade(
        market,
        start_pct=0.50,
        end_pct=0.80,
        up_threshold=0.60,
        down_threshold=0.40,
        interval_seconds=900,
    )

    assert result == {
        "traded": True,
        "side": "UP",
        "entry_price": 0.62,
        "spent": 0.615,
        "payout": 1.0,
        "profit": 0.385,
    }


def test_append_market_tape_block_appends_json_line(tmp_path) -> None:
    path = tmp_path / "market_tape.jsonl"
    block = {"slug": "btc-updown-5m-1780127400", "winner": "UP"}

    append_market_tape_block(path, block)

    assert path.read_text() == json.dumps(block) + "\n"


def test_append_close_market_log_appends_human_readable_block(tmp_path) -> None:
    path = tmp_path / "close_market.log"
    summary = {
        "timestamp": "2026-05-30T08:00:00+00:00",
        "market_slug": "btc-updown-5m-1780127400",
        "up_shares": 1.2345,
        "up_usd": 0.62,
        "down_shares": 0.5,
        "down_usd": 0.31,
        "winner": "UP",
    }

    append_close_market_log(path, summary)

    assert path.read_text() == (
        "[2026-05-30T08:00:00+00:00] Market: btc-updown-5m-1780127400\n"
        "  UP: 1.2345 shares | $0.62\n"
        "  DOWN: 0.5000 shares | $0.31\n"
        "  Winner: UP\n"
        "\n"
    )


def test_format_collateral_balance_formats_human_usdc_total() -> None:
    balance = {"raw": 6718573, "human": 6.718573}

    assert format_collateral_balance(balance) == "USDC total: $6.72"


def test_format_deposit_wallet_status_formats_wallet_and_deployed_flag() -> None:
    assert (
        format_deposit_wallet_status("0xabc", True)
        == "deposit_wallet=0xabc deployed=True"
    )


def test_build_fixed_usd_buy_request_returns_direct_market_buy_payload() -> None:
    assert build_fixed_usd_buy_request("123", 25.0, 31.5) == {
        "token_id": "123",
        "amount": 25.0,
        "user_usdc_balance": 31.5,
    }


def test_choose_trade_token_returns_mapped_token_for_up() -> None:
    assert choose_trade_token({"UP": "yes_token", "DOWN": "no_token"}, "UP") == "yes_token"


def test_prepare_live_buy_request_builds_fixed_usd_request_for_trade_side() -> None:
    decision = {"traded": True, "side": "DOWN"}
    trade_tokens = {"UP": "yes_token", "DOWN": "no_token"}

    assert prepare_live_buy_request(decision, trade_tokens, 25.0, 31.5) == {
        "token_id": "no_token",
        "amount": 25.0,
        "user_usdc_balance": 31.5,
    }


def test_prepare_live_buy_request_returns_none_when_trade_not_taken() -> None:
    decision = {"traded": False, "side": None}
    trade_tokens = {"UP": "yes_token", "DOWN": "no_token"}

    assert prepare_live_buy_request(decision, trade_tokens, 25.0, 31.5) is None


def test_load_runtime_config_reads_required_env_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRADE_WINDOW_START_PCT", "0.50")
    monkeypatch.setenv("TRADE_WINDOW_END_PCT", "0.80")
    monkeypatch.setenv("TREND_UP_THRESHOLD", "0.60")
    monkeypatch.setenv("TREND_DOWN_THRESHOLD", "0.40")
    monkeypatch.setenv("MARKET_BUY_USD", "25")
    monkeypatch.setenv("MARKET_INTERVAL_SECONDS", "900")

    assert load_runtime_config() == {
        "trade_window_start_pct": 0.50,
        "trade_window_end_pct": 0.80,
        "trend_up_threshold": 0.60,
        "trend_down_threshold": 0.40,
        "market_buy_usd": 25.0,
        "market_interval_seconds": 900,
    }
