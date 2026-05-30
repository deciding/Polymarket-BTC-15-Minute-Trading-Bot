import asyncio
from datetime import datetime, timezone

from dotenv import load_dotenv

from bot import DEFAULT_MARKET_INTERVAL
from bot import POLYMARKET
from bot import TradingNode
from bot import TradingNodeConfig
from bot import LoggingConfig
from bot import LiveDataEngineConfig
from bot import LiveExecEngineConfig
from bot import LiveRiskEngineConfig
from bot import PolymarketLiveDataClientFactory
from bot import PolymarketLiveExecClientFactory
from polymarket_node_config import build_polymarket_client_configs


def format_balance_output(balance: dict) -> str:
    total = float(balance.get("USDC", 0.0))
    free = float(balance.get("free", 0.0))
    locked = float(balance.get("locked", 0.0))
    return f"USDC total: ${total:.2f} | free: ${free:.2f} | locked: ${locked:.2f}"


def build_slug_list(market_interval: int = DEFAULT_MARKET_INTERVAL) -> list[str]:
    now = datetime.now(timezone.utc)
    interval_min = market_interval // 60
    unix_interval_start = (int(now.timestamp()) // market_interval) * market_interval
    return [
        f"btc-updown-{interval_min}m-{unix_interval_start + (i * market_interval)}"
        for i in range(-1, 3)
    ]


def extract_balance_from_node(node: TradingNode) -> dict | None:
    accounts = list(node.cache.accounts())
    if not accounts:
        return None

    account = accounts[0]
    return {
        "USDC": float(account.balance_total().as_decimal()),
        "free": float(account.balance_free().as_decimal()),
        "locked": float(account.balance_locked().as_decimal()),
    }


async def main_async() -> int:
    load_dotenv()

    _, poly_data_cfg, poly_exec_cfg = build_polymarket_client_configs(
        build_slug_list(),
        signature_type=3,
    )

    config = TradingNodeConfig(
        environment="live",
        trader_id="BTC-BALANCE-CHECK-001",
        logging=LoggingConfig(log_level="INFO", log_directory="./logs/nautilus"),
        data_engine=LiveDataEngineConfig(qsize=1000),
        exec_engine=LiveExecEngineConfig(qsize=1000),
        risk_engine=LiveRiskEngineConfig(bypass=True),
        data_clients={POLYMARKET: poly_data_cfg},
        exec_clients={POLYMARKET: poly_exec_cfg},
    )

    node = TradingNode(config=config)
    node.add_data_client_factory(POLYMARKET, PolymarketLiveDataClientFactory)
    node.add_exec_client_factory(POLYMARKET, PolymarketLiveExecClientFactory)
    node.build()

    try:
        asyncio.create_task(asyncio.to_thread(node.run))
        await asyncio.sleep(8)

        balance = extract_balance_from_node(node)
        if balance is None:
            print("No Nautilus account found in cache")
            return 1
        print(format_balance_output(balance))
        return 0
    finally:
        await node.stop_async()
        node.dispose()


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
