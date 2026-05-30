import os

from nautilus_trader.adapters.polymarket import (
    PolymarketDataClientConfig,
    PolymarketExecClientConfig,
)
from nautilus_trader.adapters.polymarket.providers import PolymarketInstrumentProviderConfig


def build_polymarket_client_configs(slugs: list[str], signature_type: int = 3):
    instrument_config = PolymarketInstrumentProviderConfig(
        load_all=True,
        filters={
            "active": True,
            "closed": False,
            "archived": False,
            "slug": tuple(slugs),
            "limit": 100,
        },
        use_gamma_markets=True,
    )

    shared_kwargs = {
        "private_key": os.getenv("POLYMARKET_PK"),
        "api_key": os.getenv("POLYMARKET_API_KEY"),
        "api_secret": os.getenv("POLYMARKET_API_SECRET"),
        "passphrase": os.getenv("POLYMARKET_PASSPHRASE"),
        "funder": os.getenv("POLYMARKET_FUNDER"),
        "signature_type": signature_type,
        "instrument_config": instrument_config,
    }

    return (
        instrument_config,
        PolymarketDataClientConfig(**shared_kwargs),
        PolymarketExecClientConfig(**shared_kwargs),
    )
