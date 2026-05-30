import asyncio
from decimal import Decimal

from dotenv import load_dotenv

from execution.polymarket_client import PolymarketClient


def format_balance_output(balance: dict) -> str:
    usdc = Decimal(str(balance.get("USDC", 0)))
    return f"USDC balance: ${usdc:.2f}"


async def main_async() -> int:
    load_dotenv()

    client = PolymarketClient()
    connected = await client.connect()
    if not connected:
        print("Failed to connect to Polymarket client")
        return 1

    try:
        balance = await client.get_balance()
        print(format_balance_output(balance))
        return 0
    finally:
        await client.disconnect()


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
