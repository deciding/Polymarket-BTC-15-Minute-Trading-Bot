from dotenv import load_dotenv

import os

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams


def format_allowance_output(data: dict) -> str:
    return f"balance={data.get('balance')} | allowances={data.get('allowances')}"


def main() -> int:
    load_dotenv()

    client = ClobClient(
        host="https://clob.polymarket.com",
        key=os.getenv("POLYMARKET_PK"),
        chain_id=137,
        signature_type=3,
        funder=os.getenv("POLYMARKET_FUNDER"),
    )
    client.set_api_creds(
        ApiCreds(
            api_key=os.getenv("POLYMARKET_API_KEY"),
            api_secret=os.getenv("POLYMARKET_API_SECRET"),
            api_passphrase=os.getenv("POLYMARKET_PASSPHRASE"),
        )
    )

    result = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=3)
    )
    print(format_allowance_output(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
