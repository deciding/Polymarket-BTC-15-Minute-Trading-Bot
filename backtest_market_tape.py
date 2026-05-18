import json
import sys
from datetime import datetime
from pathlib import Path


MARKET_SECONDS = 300


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def backtest_market(
    market,
    trade_window_start_pct,
    trade_window_end_pct,
    trend_up_threshold,
    trend_down_threshold,
):
    start_time = parse_ts(market["market_start_time"])
    window_start = MARKET_SECONDS * trade_window_start_pct
    window_end = MARKET_SECONDS * trade_window_end_pct

    for quote in market.get("quotes", []):
        quote_time = parse_ts(quote["ts"])
        elapsed = (quote_time - start_time).total_seconds()
        if not (window_start <= elapsed < window_end):
            continue

        bid = float(quote["bid"])
        ask = float(quote["ask"])
        up_price = (bid + ask) / 2.0

        if ask > trend_up_threshold:
            payout = 1.0 if market.get("winner") == "UP" else 0.0
            return {
                "traded": True,
                "side": "UP",
                "entry_price": ask,
                "spent": up_price,
                "payout": payout,
                "profit": round(payout - up_price, 2),
            }

        if bid < trend_down_threshold:
            down_price = 1.0 - up_price
            payout = 1.0 if market.get("winner") == "DOWN" else 0.0
            return {
                "traded": True,
                "side": "DOWN",
                "entry_price": bid,
                "spent": down_price,
                "payout": payout,
                "profit": round(payout - down_price, 2),
            }

    return {
        "traded": False,
        "side": None,
        "entry_price": None,
        "spent": 0.0,
        "payout": 0.0,
        "profit": 0.0,
    }


def frange(step: float):
    value = 0.5
    while value <= 1.000001:
        yield round(value, 2)
        value += step


def run_grid_search(markets, step=0.02):
    results = []
    for start_pct in frange(step):
        for end_pct in frange(step):
            if start_pct >= end_pct:
                continue
            for up_threshold in frange(step):
                if up_threshold < 0.5:
                    continue
                down_threshold = round(1.0 - up_threshold, 2)
                if down_threshold > 0.5:
                    continue

                total_spent = 0.0
                total_payout = 0.0
                total_profit = 0.0
                trades = 0
                wins = 0

                for market in markets:
                    result = backtest_market(
                        market,
                        trade_window_start_pct=start_pct,
                        trade_window_end_pct=end_pct,
                        trend_up_threshold=up_threshold,
                        trend_down_threshold=down_threshold,
                    )
                    total_spent += result["spent"]
                    total_payout += result["payout"]
                    total_profit += result["profit"]
                    if result["traded"]:
                        trades += 1
                    if result["payout"] > 0:
                        wins += 1

                results.append(
                    {
                        "trade_window_start_pct": start_pct,
                        "trade_window_end_pct": end_pct,
                        "trend_up_threshold": up_threshold,
                        "trend_down_threshold": down_threshold,
                        "trades": trades,
                        "wins": wins,
                        "win_rate": round((wins / trades), 4) if trades else 0.0,
                        "usd_spent": round(total_spent, 2),
                        "payout": round(total_payout, 2),
                        "net_profit": round(total_profit, 2),
                    }
                )

    results.sort(key=lambda row: row["net_profit"], reverse=True)
    return results


def load_markets(path: Path):
    markets = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        market = json.loads(line)
        if market.get("winner") in {"UP", "DOWN"}:
            markets.append(market)
    return markets


def main(argv):
    path = Path(argv[1]) if len(argv) > 1 else Path("market_tape.jsonl")
    step = float(argv[2]) if len(argv) > 2 else 0.02

    markets = load_markets(path)
    results = run_grid_search(markets, step=step)
    best = results[0]

    print("Best parameters:")
    print(f"  TRADE_WINDOW_START_PCT={best['trade_window_start_pct']:.2f}")
    print(f"  TRADE_WINDOW_END_PCT={best['trade_window_end_pct']:.2f}")
    print(f"  TREND_UP_THRESHOLD={best['trend_up_threshold']:.2f}")
    print(f"  TREND_DOWN_THRESHOLD={best['trend_down_threshold']:.2f}")
    print(f"  Trades={best['trades']}")
    print(f"  Wins={best['wins']}")
    print(f"  Win rate={best['win_rate']:.2%}")
    print(f"  USD spent=${best['usd_spent']:.2f}")
    print(f"  Payout=${best['payout']:.2f}")
    print(f"  Net profit=${best['net_profit']:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
