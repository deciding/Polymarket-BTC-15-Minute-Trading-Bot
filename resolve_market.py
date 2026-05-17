import json
import os
import sys
from typing import Any

import requests
from dotenv import load_dotenv


GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"


def normalize_outcome(value: str) -> str:
    outcome = value.strip().upper()
    if outcome in {"YES", "UP"}:
        return "UP"
    if outcome in {"NO", "DOWN"}:
        return "DOWN"
    return outcome


def derive_winner(market: dict[str, Any]) -> str:
    resolution = market.get("resolution")
    if isinstance(resolution, str) and resolution.strip():
        return normalize_outcome(resolution)

    tokens = market.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if token.get("winner") is True:
                outcome = token.get("outcome")
                if isinstance(outcome, str) and outcome.strip():
                    return normalize_outcome(outcome)

    outcomes_raw = market.get("outcomes")
    prices_raw = market.get("outcomePrices")
    if isinstance(outcomes_raw, str) and isinstance(prices_raw, str):
        try:
            outcomes = json.loads(outcomes_raw)
            prices = json.loads(prices_raw)
            for outcome, price in zip(outcomes, prices):
                if float(price) >= 0.999:
                    return normalize_outcome(str(outcome))
        except (ValueError, TypeError, json.JSONDecodeError):
            pass

    return "UNKNOWN"


def select_market_from_event(event: dict[str, Any], slug: str) -> dict[str, Any] | None:
    markets = event.get("markets")
    if not isinstance(markets, list):
        return None
    for market in markets:
        if isinstance(market, dict) and market.get("slug") == slug:
            return market
    return markets[0] if markets and isinstance(markets[0], dict) else None


def fetch_market(slug: str, timeout: int) -> dict[str, Any] | None:
    response = requests.get(
        GAMMA_MARKETS_URL,
        params={"slug": slug},
        timeout=timeout,
    )
    response.raise_for_status()
    markets = response.json()
    if not isinstance(markets, list) or not markets:
        event_response = requests.get(
            GAMMA_EVENTS_URL,
            params={"slug": slug},
            timeout=timeout,
        )
        event_response.raise_for_status()
        events = event_response.json()
        if not isinstance(events, list) or not events:
            return None
        market = select_market_from_event(events[0], slug)
        if market is None:
            return None
        event = events[0]
        market.setdefault("resolutionSource", event.get("resolutionSource"))
        market.setdefault("closed", event.get("closed"))
        market.setdefault("active", event.get("active"))
        market.setdefault("endDate", event.get("endDate"))
        return market
    return markets[0]


def main(argv: list[str]) -> int:
    load_dotenv()

    if len(argv) != 2:
        print("Usage: python resolve_market.py <market-slug>")
        return 1

    slug = argv[1].strip()
    timeout = int(os.getenv("POLYMARKET_RESOLUTION_TIMEOUT", "20"))

    try:
        market = fetch_market(slug, timeout)
    except requests.RequestException as exc:
        print(f"Request failed: {exc}")
        return 2

    if market is None:
        print(f"Market not found: {slug}")
        return 3

    winner = derive_winner(market)

    print(f"Slug: {market.get('slug', slug)}")
    print(f"Question: {market.get('question', 'UNKNOWN')}")
    print(f"Closed: {market.get('closed', 'UNKNOWN')}")
    print(f"Active: {market.get('active', 'UNKNOWN')}")
    print(f"End Date: {market.get('endDate', 'UNKNOWN')}")
    print(f"Resolution Source: {market.get('resolutionSource', 'UNKNOWN')}")
    print(f"Resolution: {market.get('resolution', 'UNKNOWN')}")
    print(f"Winner: {winner}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
