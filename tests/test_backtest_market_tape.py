from backtest_market_tape import backtest_market
from backtest_market_tape import run_grid_search


def test_backtest_market_takes_first_trade_in_window() -> None:
    market = {
        "market_slug": "m1",
        "market_start_time": "2026-05-17T14:40:00+00:00",
        "winner": "UP",
        "quotes": [
            {"ts": "2026-05-17T14:42:40+00:00", "bid": 0.58, "ask": 0.59},
            {"ts": "2026-05-17T14:42:43+00:00", "bid": 0.61, "ask": 0.62},
            {"ts": "2026-05-17T14:42:46+00:00", "bid": 0.80, "ask": 0.81},
        ],
    }

    result = backtest_market(
        market,
        trade_window_start_pct=0.50,
        trade_window_end_pct=0.80,
        trend_up_threshold=0.60,
        trend_down_threshold=0.40,
    )

    assert result["traded"] is True
    assert result["side"] == "UP"
    assert result["entry_price"] == 0.62
    assert result["spent"] == 0.615
    assert result["profit"] == 0.39


def test_backtest_market_prices_down_side_as_one_minus_mid() -> None:
    market = {
        "market_slug": "m2",
        "market_start_time": "2026-05-17T14:40:00+00:00",
        "winner": "DOWN",
        "quotes": [
            {"ts": "2026-05-17T14:42:40+00:00", "bid": 0.02, "ask": 0.03},
        ],
    }

    result = backtest_market(
        market,
        trade_window_start_pct=0.50,
        trade_window_end_pct=0.80,
        trend_up_threshold=0.60,
        trend_down_threshold=0.40,
    )

    assert result["traded"] is True
    assert result["side"] == "DOWN"
    assert result["spent"] == 0.975
    assert result["profit"] == 0.03


def test_run_grid_search_enforces_down_is_one_minus_up() -> None:
    markets = [
        {
            "market_slug": "m1",
            "market_start_time": "2026-05-17T14:40:00+00:00",
            "winner": "DOWN",
            "quotes": [
                {"ts": "2026-05-17T14:43:10+00:00", "bid": 0.10, "ask": 0.11},
            ],
        }
    ]

    results = run_grid_search(markets, step=0.5)

    assert results
    for row in results:
        assert row["trend_up_threshold"] >= 0.5
        assert row["trend_down_threshold"] <= 0.5
        assert round(row["trend_down_threshold"], 10) == round(1.0 - row["trend_up_threshold"], 10)
