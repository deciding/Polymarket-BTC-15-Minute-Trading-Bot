import json

from resolve_market import derive_winner
from resolve_market import select_market_from_event


def test_derive_winner_from_resolution_up() -> None:
    market = {"resolution": "UP"}
    assert derive_winner(market) == "UP"


def test_derive_winner_from_tokens_winner_flag() -> None:
    market = {
        "tokens": [
            {"outcome": "Up", "winner": True},
            {"outcome": "Down", "winner": False},
        ]
    }
    assert derive_winner(market) == "UP"


def test_derive_winner_from_outcome_prices() -> None:
    market = {
        "outcomes": json.dumps(["Up", "Down"]),
        "outcomePrices": json.dumps(["1", "0"]),
    }
    assert derive_winner(market) == "UP"


def test_derive_winner_returns_unknown_when_unresolved() -> None:
    market = {
        "outcomes": json.dumps(["Up", "Down"]),
        "outcomePrices": json.dumps(["0.61", "0.39"]),
    }
    assert derive_winner(market) == "UNKNOWN"


def test_select_market_from_event_prefers_matching_slug() -> None:
    event = {
        "markets": [
            {"slug": "other-market"},
            {"slug": "btc-updown-5m-1779014100", "question": "target"},
        ]
    }
    selected = select_market_from_event(event, "btc-updown-5m-1779014100")
    assert selected == {"slug": "btc-updown-5m-1779014100", "question": "target"}
