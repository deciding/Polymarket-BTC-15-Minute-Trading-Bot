import asyncio
import json
import logging
import os
import time

import requests
import websockets
from datetime import datetime, timezone
from dotenv import load_dotenv

from patch_clob_auth import apply_clob_auth_patch
from py_builder_relayer_client.client import RelayClient
from py_builder_signing_sdk.config import BuilderApiKeyCreds, BuilderConfig
from py_clob_client_v2 import ClobClient
from py_clob_client_v2.client import MarketOrderArgsV2, PartialCreateOrderOptions
from py_clob_client_v2.clob_types import ApiCreds, AssetType, BalanceAllowanceParams

from v2_helpers import (
    build_trade_decision,
    build_trade_decision_dual,
    format_deposit_wallet_status,
    get_current_btc_market_slug,
)

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RELAYER_URL = "https://relayer-v2.polymarket.com"
CHAIN_ID = 137


def create_market_state(market_slug: str, market_start_time: str) -> dict:
    return {
        "market_slug": market_slug,
        "market_start_time": market_start_time,
        "quotes": [],
        "trade_taken": False,
        "winner": None,
    }


def evaluate_market_for_paper_trade(
    market: dict,
    start_pct: float,
    end_pct: float,
    up: float,
    down: float,
    interval_seconds: int = 300,
) -> dict:
    return build_trade_decision(
        market,
        start_pct=start_pct,
        end_pct=end_pct,
        up_threshold=up,
        down_threshold=down,
        interval_seconds=interval_seconds,
    )


def prepare_live_buy_request(
    decision: dict,
    trade_tokens: dict[str, str],
    market_buy_usd: float,
    user_usdc_balance: float,
) -> dict | None:
    if not decision.get("traded"):
        return None
    token_id = trade_tokens.get(str(decision["side"]))
    if token_id is None:
        return None
    return {
        "token_id": token_id,
        "amount": market_buy_usd,
        "user_usdc_balance": user_usdc_balance,
    }


def load_runtime_config() -> dict:
    load_dotenv()
    return {
        "trade_window_start_pct": float(os.getenv("TRADE_WINDOW_START_PCT", "0.6")),
        "trade_window_end_pct": float(os.getenv("TRADE_WINDOW_END_PCT", "0.8")),
        "trend_up_threshold": float(os.getenv("TREND_UP_THRESHOLD", "0.6")),
        "trend_down_threshold": float(os.getenv("TREND_DOWN_THRESHOLD", "0.4")),
        "market_buy_usd": float(os.getenv("MARKET_BUY_USD", "1.0")),
        "market_interval_seconds": int(os.getenv("MARKET_INTERVAL_SECONDS", "300")),
    }


def check_deposit() -> tuple[str, bool]:
    pk = os.environ["POLYMARKET_PK"]
    api_key = os.environ["POLYMARKET_API_KEY"]
    api_secret = os.environ["POLYMARKET_API_SECRET"]
    api_passphrase = os.environ["POLYMARKET_PASSPHRASE"]

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=api_key,
            secret=api_secret,
            passphrase=api_passphrase,
        )
    )
    relayer = RelayClient(RELAYER_URL, CHAIN_ID, pk, builder_config)
    wallet = relayer.get_expected_deposit_wallet()
    deployed = relayer.get_deployed(wallet, "WALLET")
    return wallet, deployed


def check_balance(funder: str) -> float:
    pk = os.environ["POLYMARKET_PK"]
    api_key = os.environ["POLYMARKET_API_KEY"]
    api_secret = os.environ["POLYMARKET_API_SECRET"]
    api_passphrase = os.environ["POLYMARKET_PASSPHRASE"]

    apply_clob_auth_patch()

    client = ClobClient(
        host=CLOB_API,
        chain_id=CHAIN_ID,
        key=pk,
        creds=ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        ),
        signature_type=3,
        funder=funder,
    )
    result = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=3)
    )
    return float(result["balance"]) / 1_000_000


def build_clob_client(funder: str) -> ClobClient:
    apply_clob_auth_patch()
    return ClobClient(
        host=CLOB_API,
        chain_id=CHAIN_ID,
        key=os.environ["POLYMARKET_PK"],
        creds=ApiCreds(
            api_key=os.environ["POLYMARKET_API_KEY"],
            api_secret=os.environ["POLYMARKET_API_SECRET"],
            api_passphrase=os.environ["POLYMARKET_PASSPHRASE"],
        ),
        signature_type=3,
        funder=funder,
    )


def fetch_gamma_market(slug: str) -> dict | None:
    resp = requests.get(
        f"{GAMMA_API}/markets",
        params={"slug": slug},
        timeout=20,
    )
    resp.raise_for_status()
    markets = resp.json()
    return markets[0] if markets else None


def execute_buy(
    client: ClobClient,
    token_id: str,
    amount_usd: float,
    balance: float,
) -> dict | None:
    order_args = MarketOrderArgsV2(
        token_id=token_id,
        amount=amount_usd,
        side="BUY",
        user_usdc_balance=balance,
    )
    signed = client.create_market_order(
        order_args,
        options=PartialCreateOrderOptions(neg_risk=False),
    )
    resp = client.post_order(signed, "FOK", False, False)
    logger.info("Order response: %s", resp)
    return resp


class WebSocketPriceFeed:
    def __init__(
        self,
        up_token_id: str,
        down_token_id: str,
        on_quote,
    ):
        self._up_token_id = up_token_id
        self._down_token_id = down_token_id
        self._on_quote = on_quote
        self._up: dict | None = None
        self._down: dict | None = None
        self._last_merged_ms: int = 0
        self._last_sample_ts: float | None = None
        self._stop = False

    def stop(self):
        self._stop = True

    async def run(self):
        subscribe = {
            "assets_ids": [self._up_token_id, self._down_token_id],
            "type": "market",
            "custom_feature_enabled": True,
        }
        while not self._stop:
            try:
                async with websockets.connect(CLOB_WS) as ws:
                    await ws.send(json.dumps(subscribe))
                    async for raw in ws:
                        if self._stop:
                            return
                        self._handle(raw)
            except websockets.ConnectionClosed:
                if not self._stop:
                    await asyncio.sleep(1)
            except Exception:
                logger.exception("WebSocket error, reconnecting…")
                if not self._stop:
                    await asyncio.sleep(3)

    def _handle(self, raw: str):
        msg = json.loads(raw)
        if msg.get("type") != "best_bid_ask":
            return

        asset_id = msg["asset_id"]
        ts_ms = int(msg["timestamp"])
        bid = float(msg["bid"])
        ask = float(msg["ask"])

        if asset_id == self._up_token_id:
            self._up = {"ts": ts_ms, "bid": bid, "ask": ask}
        elif asset_id == self._down_token_id:
            self._down = {"ts": ts_ms, "bid": bid, "ask": ask}
        else:
            return

        if self._up is None or self._down is None:
            return

        merged_ts = max(self._up["ts"], self._down["ts"])
        if merged_ts <= self._last_merged_ms:
            return

        now_ts = time.time()
        if not (
            self._last_sample_ts is None
            or (now_ts - self._last_sample_ts) >= 3.0
        ):
            return

        self._last_merged_ms = merged_ts
        self._last_sample_ts = now_ts

        ts_iso = datetime.fromtimestamp(
            merged_ts / 1000, tz=timezone.utc
        ).isoformat()
        self._on_quote({
            "ts": ts_iso,
            "bid": self._up["bid"],
            "ask": self._up["ask"],
            "up_bid": self._up["bid"],
            "up_ask": self._up["ask"],
            "down_bid": self._down["bid"],
            "down_ask": self._down["ask"],
        })


async def run_bot():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    load_dotenv()

    config = load_runtime_config()
    interval = int(config["market_interval_seconds"])
    buy_usd = float(config["market_buy_usd"])
    start_pct = float(config["trade_window_start_pct"])
    end_pct = float(config["trade_window_end_pct"])
    up_th = float(config["trend_up_threshold"])
    down_th = float(config["trend_down_threshold"])

    logger.info("=== Deposit-wallet check ===")
    wallet, deployed = check_deposit()
    logger.info(format_deposit_wallet_status(wallet, deployed))

    funder = wallet if deployed else os.environ.get("POLYMARKET_FUNDER", wallet)
    logger.info("Funder: %s", funder)

    logger.info("=== CLOB balance check ===")
    usdc = check_balance(funder)
    logger.info("USDC balance: $%.2f", usdc)

    client = build_clob_client(funder)

    while True:
        now = datetime.now(timezone.utc)
        slug = get_current_btc_market_slug(now, interval)
        logger.info("Looking up market: %s", slug)

        market_data = fetch_gamma_market(slug)
        if market_data is None:
            logger.warning("Market not yet available, retrying in 10s…")
            await asyncio.sleep(10)
            continue

        start_time = market_data["market_start_time"]
        token_ids = json.loads(market_data["clobTokenIds"])
        up_token_id = token_ids[0]
        down_token_id = token_ids[1]
        logger.info(
            "UP=%s… DOWN=%s…",
            up_token_id[:12], down_token_id[:12],
        )

        market_state = create_market_state(slug, start_time)

        def make_on_quote(ms, cl, bal):
            def on_quote(q):
                ms["quotes"].append(q)
                up_ask = q["up_ask"]
                down_ask = q["down_ask"]
                logger.info(
                    "QUOTE up_ask=%.4f down_ask=%.4f quotes=%d taken=%s",
                    up_ask, down_ask, len(ms["quotes"]), ms["trade_taken"],
                )
                if ms["trade_taken"]:
                    return

                decision = build_trade_decision_dual(
                    ms,
                    start_pct=start_pct,
                    end_pct=end_pct,
                    up_threshold=up_th,
                    down_threshold=down_th,
                    interval_seconds=interval,
                )
                if decision["traded"]:
                    ms["trade_taken"] = True
                    side = decision["side"]
                    entry = decision["entry_price"]
                    logger.info(
                        "TRADE signal: %s at %.4f — executing buy…",
                        side, entry,
                    )
                    token_id = up_token_id if side == "UP" else down_token_id
                    execute_buy(cl, token_id, buy_usd, bal)
            return on_quote

        feed = WebSocketPriceFeed(
            up_token_id, down_token_id,
            make_on_quote(market_state, client, usdc),
        )

        logger.info("Connecting WebSocket price feed…")
        feed_task = asyncio.create_task(feed.run())
        try:
            await asyncio.sleep(interval)
        finally:
            feed.stop()
            feed_task.cancel()
            try:
                await feed_task
            except asyncio.CancelledError:
                pass

        logger.info("Market %s closed", slug)
        if market_state["trade_taken"]:
            logger.info("Trade was taken for this market")
        else:
            logger.info("No trade taken for this market — thresholds not met")

        # Wait for next interval boundary
        now_ts = time.time()
        next_boundary = ((now_ts // interval) + 1) * interval
        wait = next_boundary - now_ts
        if wait > 0:
            logger.info("Waiting %.1fs for next market…", wait)
            await asyncio.sleep(wait)


def main():
    asyncio.run(run_bot())


if __name__ == "__main__":
    main()
