import json
import os

import requests
from dotenv import load_dotenv
from py_clob_client_v2 import ClobClient
from py_clob_client_v2.client import PartialCreateOrderOptions
from py_clob_client_v2.client import MarketOrderArgsV2
from py_clob_client_v2.clob_types import ApiCreds, AssetType, BalanceAllowanceParams


def format_market_summary(slug: str, token_id: str, outcome_price: str) -> str:
    return f"slug={slug} token_id={token_id} outcome_price={outcome_price}"


def main() -> int:
    load_dotenv('.env')

    market_interval = 300
    import datetime as _dt
    now = _dt.datetime.now(_dt.timezone.utc)
    ts = (int(now.timestamp()) // market_interval) * market_interval
    slug = f"btc-updown-5m-{ts}"

    response = requests.get(
        'https://gamma-api.polymarket.com/markets',
        params={'slug': slug},
        timeout=20,
    )
    response.raise_for_status()
    markets = response.json()
    if not markets:
        print(f"No active market found for slug {slug}")
        return 1

    market = markets[0]
    token_ids = json.loads(market['clobTokenIds'])
    outcome_prices = json.loads(market['outcomePrices'])
    up_token = token_ids[0]
    up_price = outcome_prices[0]
    print(format_market_summary(slug, up_token, up_price))

    client = ClobClient(
        host='https://clob.polymarket.com',
        chain_id=137,
        key=os.getenv('POLYMARKET_PK'),
        creds=ApiCreds(
            api_key=os.getenv('POLYMARKET_API_KEY'),
            api_secret=os.getenv('POLYMARKET_API_SECRET'),
            api_passphrase=os.getenv('POLYMARKET_PASSPHRASE'),
        ),
        signature_type=3,
        funder=os.getenv('POLYMARKET_FUNDER'),
    )

    balance = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=3)
    )
    user_balance = int(balance['balance']) / 1_000_000

    # Tiny market buy for smoke testing the auth/signer path.
    order_args = MarketOrderArgsV2(
        token_id=up_token,
        amount=1.0,
        side='BUY',
        user_usdc_balance=user_balance,
    )
    signed = client.create_market_order(order_args, options=PartialCreateOrderOptions(neg_risk=False))
    print(signed)
    print(client.post_order(signed, 'FOK', False, False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
