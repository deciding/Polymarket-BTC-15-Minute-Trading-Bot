import asyncio
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
import math
from decimal import Decimal
import time
from dataclasses import dataclass
from typing import List, Optional, Dict
import random

# Add project to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


try:
    from patch_gamma_markets import apply_gamma_markets_patch, verify_patch
    patch_applied = apply_gamma_markets_patch()
    if patch_applied:
        verify_patch()
    else:
        print("ERROR: Failed to apply gamma_market patch")
        sys.exit(1)
except ImportError as e:
    print(f"ERROR: Could not import patch module: {e}")
    print("Make sure patch_gamma_markets.py is in the same directory")
    sys.exit(1)

# Now import Nautilus
from nautilus_trader.config import (
    LiveDataEngineConfig,
    LiveExecEngineConfig,
    LiveRiskEngineConfig,
    LoggingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.adapters.polymarket import POLYMARKET
from nautilus_trader.adapters.polymarket import (
    PolymarketDataClientConfig,
    PolymarketExecClientConfig,
)
from nautilus_trader.adapters.polymarket.factories import (
    PolymarketLiveDataClientFactory,
    PolymarketLiveExecClientFactory,
)
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.identifiers import InstrumentId, ClientOrderId
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.data import QuoteTick

from dotenv import load_dotenv
from loguru import logger
import redis




def get_market_winner_sync(market_slug: str, last_price: float = None) -> str:
    """Resolve a market winner from Gamma API market/event payloads."""
    import requests

    def normalize_outcome(value: str) -> str:
        outcome = value.strip().upper()
        if outcome in {"YES", "UP"}:
            return "UP"
        if outcome in {"NO", "DOWN"}:
            return "DOWN"
        return outcome

    def derive_winner(market: dict) -> str:
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

    def select_market_from_event(event: dict, slug: str) -> Optional[dict]:
        markets = event.get("markets")
        if not isinstance(markets, list):
            return None
        for market in markets:
            if isinstance(market, dict) and market.get("slug") == slug:
                return market
        return markets[0] if markets and isinstance(markets[0], dict) else None

    try:
        market_response = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"slug": market_slug},
            timeout=15,
        )
        market_response.raise_for_status()
        markets = market_response.json()
        if isinstance(markets, list) and markets:
            market = markets[0]
            logger.info(f"  Found market via gamma-api markets: closed={market.get('closed')}")
            winner = derive_winner(market)
            if winner != "UNKNOWN":
                logger.info(f"  Winner source: market payload")
                return winner

        logger.info("  Market not resolved via /markets, trying /events fallback")
        event_response = requests.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": market_slug},
            timeout=15,
        )
        event_response.raise_for_status()
        events = event_response.json()
        if isinstance(events, list) and events:
            event = events[0]
            market = select_market_from_event(event, market_slug)
            if market:
                market.setdefault("resolutionSource", event.get("resolutionSource"))
                market.setdefault("closed", event.get("closed"))
                market.setdefault("active", event.get("active"))
                market.setdefault("endDate", event.get("endDate"))
                winner = derive_winner(market)
                if winner != "UNKNOWN":
                    logger.info("  Winner source: event markets outcomePrices/tokens")
                    return winner

        logger.info("  API did not yield a winner, trying last_price fallback")
        if last_price is not None:
            logger.info(f"  Using last known price: {last_price}")
            if last_price >= 0.5:
                return "UP"
            return "DOWN"
        
        logger.warning(f"  Market not found and no last_price available")
        return "UNKNOWN"
    except Exception as e:
        logger.warning(f"Could not fetch market winner for {market_slug}: {e}")
        
        # Fallback: use last known price
        if last_price is not None:
            logger.info(f"  Using last known price as fallback: {last_price}")
            if last_price >= 0.5:
                return "UP"
            return "DOWN"
        
        return "UNKNOWN"


async def _poll_market_resolution_for_slug(self, market_slug: str):
    """Poll for market resolution async with retries"""
    max_retries = 6
    retry_delay = 10  # seconds
    
    logger.info(f"  Polling for resolution (max {max_retries} tries)...")
    
    for attempt in range(max_retries):
        winner = get_market_winner_sync(market_slug)
        if winner != "UNKNOWN":
            logger.info(f"  Resolution found on attempt {attempt + 1}: {winner}")
            return winner
        
        if attempt < max_retries - 1:
            logger.info(f"  Attempt {attempt + 1} failed, retrying in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
    
    logger.warning(f"  Could not resolve after {max_retries} attempts")
    return "UNKNOWN"


# Import our phases
from core.strategy_brain.signal_processors.spike_detector import SpikeDetectionProcessor
from core.strategy_brain.signal_processors.sentiment_processor import SentimentProcessor
from core.strategy_brain.signal_processors.divergence_processor import PriceDivergenceProcessor
from core.strategy_brain.signal_processors.orderbook_processor import OrderBookImbalanceProcessor
from core.strategy_brain.signal_processors.tick_velocity_processor import TickVelocityProcessor
from core.strategy_brain.signal_processors.deribit_pcr_processor import DeribitPCRProcessor
from core.strategy_brain.fusion_engine.signal_fusion import get_fusion_engine
from execution.risk_engine import get_risk_engine
from monitoring.performance_tracker import get_performance_tracker
from monitoring.grafana_exporter import get_grafana_exporter
from feedback.learning_engine import get_learning_engine
from polymarket_node_config import build_polymarket_client_configs
load_dotenv()
from patch_market_orders import apply_market_order_patch
patch_applied = apply_market_order_patch()
if patch_applied:
    logger.info("Market order patch applied successfully")
else:
    logger.warning("Market order patch failed - orders may be rejected")


# =============================================================================
# CONSTANTS
# =============================================================================
QUOTE_STABILITY_REQUIRED = 3      # Need only 3 valid ticks to be stable (faster startup)
QUOTE_MIN_SPREAD = 0.001          # Both bid and ask must be at least this
DEFAULT_MARKET_INTERVAL = 300     # 5-minute markets (use --interval to change to 15min=900)

# Strategy thresholds from environment
def get_strategy_thresholds():
    return {
        "trade_window_start_pct": float(os.getenv("TRADE_WINDOW_START_PCT", "0.6")),
        "trade_window_end_pct": float(os.getenv("TRADE_WINDOW_END_PCT", "0.8")),
        "trend_up_threshold": float(os.getenv("TREND_UP_THRESHOLD", "0.60")),
        "trend_down_threshold": float(os.getenv("TREND_DOWN_THRESHOLD", "0.40")),
        "enable_signals": os.getenv("ENABLE_SIGNALS", "true").lower() == "true",
    }


@dataclass
class PaperTrade:
    """Track paper/simulation trades"""
    timestamp: datetime
    direction: str
    size_usd: float
    price: float
    signal_score: float
    signal_confidence: float
    outcome: str = "PENDING"

    def to_dict(self):
        return {
            'timestamp': self.timestamp.isoformat(),
            'direction': self.direction,
            'size_usd': self.size_usd,
            'price': self.price,
            'signal_score': self.signal_score,
            'signal_confidence': self.signal_confidence,
            'outcome': self.outcome,
        }


@dataclass
class RealtimePaperPosition:
    """Track real-time paper trades with actual shares and P&L calculation"""
    entry_time: datetime
    direction: str          # "long" (bought UP/YES) or "short" (bought DOWN/NO)
    entry_price: float     # Price when entered (e.g., 0.72)
    usd_spent: float       # How much USD spent (e.g., 1.00)
    shares: float         # Number of shares bought (usd_spent / entry_price)
    market_slug: str      # Which market
    market_end_time: datetime  # When market closes
    
    def calculate_pnl(self, exit_price: float) -> float:
        """Calculate P&L based on exit price"""
        if self.direction == "long":
            # Bought UP/YES, profit if price goes up
            pnl = (exit_price - self.entry_price) * self.shares
        else:
            # Bought DOWN/NO, profit if price goes down
            pnl = (self.entry_price - exit_price) * self.shares
        return pnl
    
    def to_dict(self):
        return {
            'entry_time': self.entry_time.isoformat(),
            'direction': self.direction,
            'entry_price': self.entry_price,
            'usd_spent': self.usd_spent,
            'shares': self.shares,
            'market_slug': self.market_slug,
            'market_end_time': self.market_end_time.isoformat(),
        }


def init_redis():
    """Initialize Redis connection for simulation mode control."""
    try:
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 2)),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True
        )
        redis_client.ping()
        logger.info("Redis connection established")
        return redis_client
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")
        logger.warning("Simulation mode will be static (from .env)")
        return None


class IntegratedBTCStrategy(Strategy):
    """
    Integrated BTC Strategy - FIXED VERSION
    - Subscribes immediately at startup
    - Forces stability for first trade
    - Correct timing for market switching
    """

    def __init__(self, redis_client=None, enable_grafana=True, test_mode=False, market_interval: int = DEFAULT_MARKET_INTERVAL, thresholds: dict = None):
        super().__init__()

        self.market_interval = market_interval
        
        # Load thresholds from env or use defaults
        if thresholds is None:
            thresholds = get_strategy_thresholds()
        self.trade_window_start_pct = thresholds["trade_window_start_pct"]
        self.trade_window_end_pct = thresholds["trade_window_end_pct"]
        self.trend_up_threshold = thresholds["trend_up_threshold"]
        self.trend_down_threshold = thresholds["trend_down_threshold"]
        self.enable_signals = thresholds["enable_signals"]
        
        self.bot_start_time = datetime.now(timezone.utc)
        self.restart_after_minutes = float(os.getenv("RESTART_AFTER_MINUTES", "90"))

        # Nautilus
        self.instrument_id = None
        self.redis_client = redis_client
        self.current_simulation_mode = False

        # Store ALL BTC instruments
        self.all_btc_instruments: List[Dict] = []
        self.current_instrument_index: int = -1
        self.next_switch_time: Optional[datetime] = None

        # Quote-stability tracking
        self._stable_tick_count = 0
        self._market_stable = False
        self._last_instrument_switch = None
        
        # =========================================================================
        # FIX 1: Force first trade by setting last_trade_time to -1
        # =========================================================================
        self.last_trade_time = -1  # Force first trade immediately!
        self._waiting_for_market_open = False  # True when waiting for a future market to open
        self._last_bid_ask = None  # (bid_decimal, ask_decimal) from last tick, for liquidity checks

        # Tick buffer: rolling 90s of ticks for TickVelocityProcessor
        from collections import deque
        self._tick_buffer: deque = deque(maxlen=500)  # ~500 ticks = well over 90s

        # YES token id for the current market (set in _load_all_btc_instruments)
        self._yes_token_id: Optional[str] = None

        # Phase 4: Signal Processors
        self.spike_detector = SpikeDetectionProcessor(
            spike_threshold=0.05,       # FIXED: was 0.15 (too high for probabilities)
            lookback_periods=20,
        )
        self.sentiment_processor = SentimentProcessor(
            extreme_fear_threshold=25,
            extreme_greed_threshold=75,
        )
        self.divergence_processor = PriceDivergenceProcessor(
            divergence_threshold=0.05,
        )
        self.orderbook_processor = OrderBookImbalanceProcessor(
            imbalance_threshold=0.30,   # 30% skew to signal
            min_book_volume=50.0,       # ignore illiquid books
        )
        self.tick_velocity_processor = TickVelocityProcessor(
            velocity_threshold_60s=0.015,  # 1.5% move in 60s
            velocity_threshold_30s=0.010,  # 1.0% move in 30s
        )
        self.deribit_pcr_processor = DeribitPCRProcessor(
            bullish_pcr_threshold=1.20,
            bearish_pcr_threshold=0.70,
            max_days_to_expiry=2,
            cache_seconds=300,          # refresh every 5 min
        )

        # Phase 4: Signal Fusion — update weights for 6 processors
        self.fusion_engine = get_fusion_engine()
        # Rebalanced weights (must sum ≤ 1.0; higher = more influence)
        self.fusion_engine.set_weight("OrderBookImbalance", 0.30)  # best real-time signal
        self.fusion_engine.set_weight("TickVelocity",       0.25)  # fast poly momentum
        self.fusion_engine.set_weight("PriceDivergence",    0.18)  # spot momentum
        self.fusion_engine.set_weight("SpikeDetection",     0.12)  # mean reversion
        self.fusion_engine.set_weight("DeribitPCR",         0.10)  # institutional sentiment
        self.fusion_engine.set_weight("SentimentAnalysis",  0.05)  # daily F&G (weak)

        # Phase 5: Risk Management
        self.risk_engine = get_risk_engine()

        # Phase 6: Performance Tracking
        self.performance_tracker = get_performance_tracker()

        # Phase 7: Learning Engine
        self.learning_engine = get_learning_engine()

        # Phase 6: Grafana (optional)
        if enable_grafana:
            self.grafana_exporter = get_grafana_exporter()
        else:
            self.grafana_exporter = None

        # Price history
        self.price_history = []
        self.max_history = 100

        # Paper trading tracker
        self.paper_trades: List[PaperTrade] = []
        
        # Realtime paper positions (enhanced tracking)
        # Cumulative shares for UP and DOWN (accumulated per market)
        self.up_shares: float = 0.0
        self.up_usd_spent: float = 0.0
        self.down_shares: float = 0.0
        self.down_usd_spent: float = 0.0
        self.current_market_slug: str = ""
        
        # Previous market info (for resolution check after switch)
        self.last_market_slug: str = ""
        self.last_up_shares: float = 0.0
        self.last_up_usd: float = 0.0
        self.last_down_shares: float = 0.0
        self.last_down_usd: float = 0.0
        self.last_market_price: float = None
        self.market_tape_path: str = "market_tape.jsonl"
        self.current_market_tape: Dict = {}
        self.last_market_tape: Dict = {}

        self.test_mode = test_mode

        if test_mode:
            logger.info("=" * 80)
            logger.info("  TEST MODE ACTIVE - Trading every minute!")
            logger.info("=" * 80)

        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY INITIALIZED - FIXED VERSION")
        logger.info("  Phase 4: Signal processors ready")
        logger.info("  Phase 5: Risk engine ready")
        logger.info("  Phase 6: Performance tracking ready")
        logger.info("  Phase 7: Learning engine ready")
        logger.info("  $1 per trade maximum")
        if self.restart_after_minutes > 0:
            logger.info(f"  Auto-restart after {self.restart_after_minutes:.0f} minutes")
        else:
            logger.info("  Auto-restart disabled")
        logger.info("=" * 80)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _seconds_to_next_15min_boundary(self) -> float:
        """Return seconds until the next 15-minute UTC boundary."""
        now_ts = datetime.now(timezone.utc).timestamp()
        next_boundary = (math.floor(now_ts / self.market_interval) + 1) * self.market_interval
        return next_boundary - now_ts

    def _is_quote_valid(self, bid, ask) -> bool:
        """Return True only when BOTH bid and ask are present and make sense."""
        if bid is None or ask is None:
            return False
        try:
            b = float(bid)
            a = float(ask)
        except (TypeError, ValueError):
            return False
        if b < QUOTE_MIN_SPREAD or a < QUOTE_MIN_SPREAD:
            return False
        if b > 0.999 or a > 0.999:
            return False
        return True

    def _reset_stability(self, reason: str = ""):
        """Mark the market as unstable and reset the counter."""
        if self._market_stable:
            logger.warning(f"Market stability RESET{' – ' + reason if reason else ''}")
        self._market_stable = False
        self._stable_tick_count = 0

    # ------------------------------------------------------------------
    # Redis
    # ------------------------------------------------------------------

    async def check_simulation_mode(self) -> bool:
        """Check Redis for current simulation mode."""
        if not self.redis_client:
            return self.current_simulation_mode
        try:
            sim_mode = self.redis_client.get('btc_trading:simulation_mode')
            if sim_mode is not None:
                redis_simulation = sim_mode == '1'
                if redis_simulation != self.current_simulation_mode:
                    self.current_simulation_mode = redis_simulation
                    mode_text = "SIMULATION" if redis_simulation else "LIVE TRADING"
                    logger.warning(f"Trading mode changed to: {mode_text}")
                    if not redis_simulation:
                        logger.warning("LIVE TRADING ACTIVE - Real money at risk!")
                return redis_simulation
        except Exception as e:
            logger.warning(f"Failed to check Redis simulation mode: {e}")
        return self.current_simulation_mode

    # ------------------------------------------------------------------
    # Strategy lifecycle
    # ------------------------------------------------------------------

    def on_start(self):
        """Called when strategy starts - LOAD ALL MARKETS AND SUBSCRIBE IMMEDIATELY"""
        logger.info("=" * 80)
        logger.info("INTEGRATED BTC STRATEGY STARTED - FIXED VERSION")
        logger.info("=" * 80)

        # =========================================================================
        # FIX 2: Load ALL BTC instruments at startup
        # =========================================================================
        self._load_all_btc_instruments()

        # =========================================================================
        # FIX 3: Force subscribe to current market IMMEDIATELY
        # =========================================================================
        if self.instrument_id:
            self.subscribe_quote_ticks(self.instrument_id)
            logger.info(f"✓ SUBSCRIBED to market: {self.instrument_id}")
            
            # Try to get current price from cache
            try:
                quote = self.cache.quote_tick(self.instrument_id)
                if quote and quote.bid_price and quote.ask_price:
                    current_price = (quote.bid_price + quote.ask_price) / 2
                    self.price_history.append(current_price)
                    logger.info(f"✓ Initial price: ${float(current_price):.4f}")
            except Exception as e:
                logger.debug(f"No initial price yet: {e}")

        # Generate synthetic history if needed
        if len(self.price_history) < 20:
            self._generate_synthetic_history(target_count=20, existing_count=len(self.price_history))

        # =========================================================================
        # FIX 4: Start the timer loop (but don't rely on it for trading)
        # =========================================================================
        self.run_in_executor(self._start_timer_loop)

        if self.grafana_exporter:
            import threading
            threading.Thread(target=self._start_grafana_sync, daemon=True).start()

        logger.info("=" * 80)
        logger.info("Strategy active - will trade every 15 minutes")
        logger.info(f"Price history: {len(self.price_history)} points")
        if len(self.price_history) >= 20:
            logger.info("✓ READY TO TRADE NOW!")
        else:
            logger.warning(f"⚠ Need more history ({len(self.price_history)}/20)")
        logger.info("=" * 80)

    def _generate_synthetic_history(self, target_count: int = 20, existing_count: int = 0):
        """Generate synthetic price history for testing"""
        if self.price_history:
            base_price = self.price_history[-1]
        else:
            base_price = Decimal("0.5")
        needed = target_count - existing_count
        if needed <= 0:
            return
        for _ in range(needed):
            change = Decimal(str(random.uniform(-0.03, 0.03)))
            new_price = base_price * (Decimal("1.0") + change)
            new_price = max(Decimal("0.01"), min(Decimal("0.99"), new_price))
            self.price_history.append(new_price)
            base_price = new_price

    # ------------------------------------------------------------------
    # Load all BTC instruments at once
    # ------------------------------------------------------------------

    def _load_all_btc_instruments(self):
        """Load ALL BTC instruments from cache and sort by start time"""
        instruments = self.cache.instruments()
        logger.info(f"Loading ALL BTC instruments from {len(instruments)} total...")
        
        now = datetime.now(timezone.utc)
        current_timestamp = int(now.timestamp())
        
        btc_instruments = []
        
        interval_suffix = f"{self.market_interval // 60}m"
        for instrument in instruments:
            try:
                if hasattr(instrument, 'info') and instrument.info:
                    question = instrument.info.get('question', '').lower()
                    slug = instrument.info.get('market_slug', '').lower()
                    
                    if ('btc' in question or 'btc' in slug) and interval_suffix in slug:
                        try:
                            timestamp_part = slug.split('-')[-1]
                            market_timestamp = int(timestamp_part)
                            
                            # The slug timestamp IS the market start time (Unix, no offset).
                            # end_date_iso is a DATE-only string (e.g. "2026-02-20"), NOT a datetime,
                            # so parsing it gives midnight UTC which is wrong for intraday markets.
                            # Always derive end_timestamp from the slug: start + interval.
                            real_start_ts = market_timestamp
                            end_timestamp = market_timestamp + self.market_interval
                            time_diff = real_start_ts - current_timestamp
                            
                            # Only include markets that haven't ended yet
                            if end_timestamp > current_timestamp:
                                # Extract YES token ID for CLOB order book API.
                                # Nautilus instrument ID format:
                                #   {condition_id}-{token_id}.POLYMARKET
                                # The CLOB /book endpoint only accepts the token_id
                                # (the part after the dash, before .POLYMARKET).
                                raw_id = str(instrument.id)
                                # Strip .POLYMARKET suffix first
                                without_suffix = raw_id.split('.')[0] if '.' in raw_id else raw_id
                                # Then take the token_id after the condition_id dash
                                yes_token_id = without_suffix.split('-')[-1] if '-' in without_suffix else without_suffix

                                btc_instruments.append({
                                    'instrument': instrument,
                                    'slug': slug,
                                    'start_time': datetime.fromtimestamp(real_start_ts, tz=timezone.utc),
                                    'end_time': datetime.fromtimestamp(end_timestamp, tz=timezone.utc),
                                    'market_timestamp': market_timestamp,
                                    'end_timestamp': end_timestamp,
                                    'time_diff_minutes': time_diff / 60,
                                    'yes_token_id': yes_token_id,
                                })
                        except (ValueError, IndexError):
                            continue
            except Exception:
                continue
        
        # Pair YES and NO tokens by slug.
        # Each Polymarket market has two tokens loaded as separate Nautilus instruments.
        # The first instrument found for a slug is stored as the primary (YES/UP).
        # The second instrument found for the same slug is the NO/DOWN token.
        seen_slugs = {}
        deduped = []
        for inst in btc_instruments:
            slug = inst['slug']
            if slug not in seen_slugs:
                # First token seen = YES (UP)
                inst['yes_instrument_id'] = inst['instrument'].id
                inst['no_instrument_id'] = None  # will be filled when second token found
                seen_slugs[slug] = inst
                deduped.append(inst)
            else:
                # Second token seen = NO (DOWN) — store it on the existing entry
                seen_slugs[slug]['no_instrument_id'] = inst['instrument'].id
        btc_instruments = deduped
        
        # Sort by start time (absolute timestamp, not time-of-day)
        btc_instruments.sort(key=lambda x: x['market_timestamp'])
        
        logger.info("=" * 80)
        logger.info(f"FOUND {len(btc_instruments)} BTC 15-MIN MARKETS:")
        for i, inst in enumerate(btc_instruments):
            # A market is ACTIVE if it has started AND not yet ended
            is_active = inst['time_diff_minutes'] <= 0 and inst['end_timestamp'] > current_timestamp
            status = "ACTIVE" if is_active else "FUTURE" if inst['time_diff_minutes'] > 0 else "PAST"
            logger.info(f"  [{i}] {inst['slug']}: {status} (starts at {inst['start_time'].strftime('%H:%M:%S')}, ends at {inst['end_time'].strftime('%H:%M:%S')})")
        logger.info("=" * 80)
        
        self.all_btc_instruments = btc_instruments
        
        # Find current market and SUBSCRIBE IMMEDIATELY
        # FIXED: A market is current if it has STARTED and not yet ENDED (use end_time, not a hardcoded 15-min window)
        for i, inst in enumerate(btc_instruments):
            is_active = inst['time_diff_minutes'] <= 0 and inst['end_timestamp'] > current_timestamp
            if is_active:
                self.current_instrument_index = i
                self.instrument_id = inst['instrument'].id
                self.next_switch_time = inst['end_time']
                self._yes_token_id = inst.get('yes_token_id')
                self._yes_instrument_id = inst.get('yes_instrument_id', inst['instrument'].id)
                self._no_instrument_id = inst.get('no_instrument_id')
                logger.info(f"✓ CURRENT MARKET: {inst['slug']} (index {i})")
                logger.info(f"  Next switch at: {self.next_switch_time.strftime('%H:%M:%S')}")
                logger.info(f"  YES token: {self._yes_token_id[:16]}…" if self._yes_token_id else "  YES token: unknown")
                
                # =========================================================================
                # CRITICAL FIX: Subscribe immediately!
                # =========================================================================
                self.subscribe_quote_ticks(self.instrument_id)
                logger.info(f"  ✓ SUBSCRIBED to current market")
                break
        
        if self.current_instrument_index == -1 and btc_instruments:
            # No currently-active market — find the NEAREST upcoming one
            # (smallest positive time_diff_minutes = starts soonest)
            future_markets = [inst for inst in btc_instruments if inst['time_diff_minutes'] > 0]
            if future_markets:
                nearest = min(future_markets, key=lambda x: x['time_diff_minutes'])
                nearest_idx = btc_instruments.index(nearest)
            else:
                # All markets are in the past — use the last one
                nearest = btc_instruments[-1]
                nearest_idx = len(btc_instruments) - 1

            self.current_instrument_index = nearest_idx
            inst = nearest
            self.instrument_id = inst['instrument'].id
            self._yes_token_id = inst.get('yes_token_id')
            self._yes_instrument_id = inst.get('yes_instrument_id', inst['instrument'].id)
            self._no_instrument_id = inst.get('no_instrument_id')
            self.next_switch_time = inst['start_time']  # switch_time = when it OPENS
            logger.info(f"⚠ NO CURRENT MARKET - WAITING FOR NEAREST FUTURE: {inst['slug']}")
            logger.info(f"  Starts in {inst['time_diff_minutes']:.1f} min at {self.next_switch_time.strftime('%H:%M:%S')} UTC")

            # Subscribe so we get ticks when it opens
            self.subscribe_quote_ticks(self.instrument_id)
            logger.info(f"  ✓ SUBSCRIBED to future market")
            # Block trading until the market actually opens (timer loop sets _market_open flag)
            self._waiting_for_market_open = True
            
    def _switch_to_next_market(self):
        """Switch to the next market in the pre-loaded list"""
        if not self.all_btc_instruments:
            logger.error("No instruments loaded!")
            return False
        
        next_index = self.current_instrument_index + 1
        if next_index >= len(self.all_btc_instruments):
            logger.warning("No more markets available - will restart bot")
            return False
        
        next_market = self.all_btc_instruments[next_index]
        now = datetime.now(timezone.utc)
        
        # Check if next market is ready
        if now < next_market['start_time']:
            logger.info(f"Waiting for next market at {next_market['start_time'].strftime('%H:%M:%S')}")
            return False
        
        # Switch to next market
        self.current_instrument_index = next_index
        self.instrument_id = next_market['instrument'].id
        self.next_switch_time = next_market['end_time']
        self._yes_token_id = next_market.get('yes_token_id')
        self._yes_instrument_id = next_market.get('yes_instrument_id', next_market['instrument'].id)
        self._no_instrument_id = next_market.get('no_instrument_id')
        
        logger.info("=" * 80)
        logger.info(f"SWITCHING TO NEXT MARKET: {next_market['slug']}")
        logger.info(f"  Current time: {now.strftime('%H:%M:%S')}")
        logger.info(f"  Market ends at: {self.next_switch_time.strftime('%H:%M:%S')}")
        logger.info("=" * 80)
        
        # =========================================================================
        # FIX 5: Force stability for new market and reset trade timer correctly
        # =========================================================================
        self._stable_tick_count = QUOTE_STABILITY_REQUIRED  # Force stable immediately
        self._market_stable = True
        self._waiting_for_market_open = False  # Market is now active
        
        # Reset trade timer so we trade at the NEXT quote we receive
        # Use -1 so any interval will trigger (same as startup)
        self.last_trade_time = -1
        logger.info(f"  Trade timer reset — will trade on next tick")
        
        # Save last market info for resolution check after switch
        if self.up_shares > 0 or self.down_shares > 0:
            self.last_market_slug = self.current_market_slug
            self.last_up_shares = self.up_shares
            self.last_up_usd = self.up_usd_spent
            self.last_down_shares = self.down_shares
            self.last_down_usd = self.down_usd_spent
            # Save last price for fallback resolution
            self.last_market_price = float(self.price_history[-1]) if self.price_history else None
            self.last_market_tape = self.current_market_tape.copy() if self.current_market_tape else {}
            
            # Reset for next market
            self.up_shares = 0.0
            self.up_usd_spent = 0.0
            self.down_shares = 0.0
            self.down_usd_spent = 0.0
            self.current_market_slug = ""
            self.current_market_tape = {}
        
        # Switch to new market first
        self.subscribe_quote_ticks(self.instrument_id)
        
        # After switch, poll for last market resolution and write to log
        if self.last_market_slug:
            self._poll_and_write_resolution()
        return True

    # ------------------------------------------------------------------
    # Timer loop - SIMPLIFIED
    # ------------------------------------------------------------------

    def _start_timer_loop(self):
        """Start timer loop in executor"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._timer_loop())
        finally:
            loop.close()

    async def _timer_loop(self):
        """
        Timer loop: checks every 10 seconds if it's time to switch markets.
        Also handles the case where we're waiting for a future market to open.
        """
        while True:
            # --- auto-restart check ---
            uptime_minutes = (datetime.now(timezone.utc) - self.bot_start_time).total_seconds() / 60
            if self.restart_after_minutes > 0 and uptime_minutes >= self.restart_after_minutes:
                logger.warning("AUTO-RESTART TIME - Loading fresh filters")
                import signal as _signal
                os.kill(os.getpid(), _signal.SIGTERM)
                return

            now = datetime.now(timezone.utc)

            if self.next_switch_time and now >= self.next_switch_time:
                if self._waiting_for_market_open:
                    # The future market we were waiting for has now opened
                    # Treat it like a market switch so trade timer resets
                    logger.info("=" * 80)
                    logger.info(f"⏰ WAITING MARKET NOW OPEN: {now.strftime('%H:%M:%S')} UTC")
                    logger.info("=" * 80)
                    # Update next_switch_time to the market's END time
                    if (self.current_instrument_index >= 0 and
                            self.current_instrument_index < len(self.all_btc_instruments)):
                        current_market = self.all_btc_instruments[self.current_instrument_index]
                        self.next_switch_time = current_market['end_time']
                        logger.info(f"  Market ends at {self.next_switch_time.strftime('%H:%M:%S')} UTC")
                    self._waiting_for_market_open = False
                    self._market_stable = True
                    self._stable_tick_count = QUOTE_STABILITY_REQUIRED
                    self.last_trade_time = -1  # Trade immediately on next tick
                    logger.info("  ✓ MARKET OPEN — ready to trade on next tick")
                else:
                    # Normal market switch
                    self._switch_to_next_market()

            await asyncio.sleep(10)

    # ------------------------------------------------------------------
    # Quote tick handler - SIMPLIFIED
    # ------------------------------------------------------------------

    def on_quote_tick(self, tick: QuoteTick):
        """Handle quote tick - TRADE when market opens and at each 5-min boundary"""
        try:
            # Only process ticks from current instrument
            if self.instrument_id is None or tick.instrument_id != self.instrument_id:
                return

            now = datetime.now(timezone.utc)
            bid = tick.bid_price
            ask = tick.ask_price

            if bid is None or ask is None:
                return
                
            try:
                bid_decimal = bid.as_decimal()
                ask_decimal = ask.as_decimal()
            except:
                return

            # Print ongoing quote every 3 seconds
            if not hasattr(self, '_last_price_print_time') or (now.timestamp() - self._last_price_print_time) >= 3:
                mid_price = (bid_decimal + ask_decimal) / 2
                logger.info(f"[PRICE] {now.strftime('%H:%M:%S')} | Bid: ${float(bid_decimal):.4f} | Ask: ${float(ask_decimal):.4f} | Mid: ${float(mid_price):.4f}")
                self._last_price_print_time = now.timestamp()
                if self.current_market_tape:
                    self.current_market_tape.setdefault("quotes", []).append({
                        "ts": now.isoformat(),
                        "bid": float(bid_decimal),
                        "ask": float(ask_decimal),
                    })

            # Always store price history
            mid_price = (bid_decimal + ask_decimal) / 2
            self.price_history.append(mid_price)
            if len(self.price_history) > self.max_history:
                self.price_history.pop(0)
            
            # Store latest bid/ask for liquidity check before order placement
            self._last_bid_ask = (bid_decimal, ask_decimal)

            # Tick buffer for TickVelocityProcessor (rolling 90s window)
            self._tick_buffer.append({'ts': now, 'price': mid_price})

            # Stability gate
            if not self._market_stable:
                self._stable_tick_count += 1
                if self._stable_tick_count >= 1:
                    self._market_stable = True
                    logger.info(f"✓ Market STABLE immediately")
                else:
                    return

            # =========================================================================
            # FIXED TRADING LOGIC:
            # 
            # We trade once per 15-min market interval.
            # Instead of checking wall-clock 15-min boundaries (which caused the 2-hour
            # wait), we use a simple counter keyed to the Polymarket market's OWN
            # start time.
            #
            # The market's start_time is stored in all_btc_instruments[current_index].
            # Within each market, we compute a "sub-interval" index:
            #   sub_interval = elapsed_seconds_since_market_open // 900
            # Trade ID = (market_start_timestamp, sub_interval)
            # This fires once at market open AND once after every 15 min within
            # the same market if it's a multi-interval market.
            #
            # If _waiting_for_market_open is True (started before market opens),
            # we block trading until the timer loop calls _switch_to_next_market.
            # =========================================================================

            # Block trading if waiting for a future market to open
            if self._waiting_for_market_open:
                return

            # Get current market info
            if (self.current_instrument_index < 0 or
                    self.current_instrument_index >= len(self.all_btc_instruments)):
                return

            current_market = self.all_btc_instruments[self.current_instrument_index]
            market_start_ts = current_market['market_timestamp']  # Slug timestamp = market start (Unix)

            # How many 5-min intervals have elapsed since this market opened?
            elapsed_secs = now.timestamp() - market_start_ts
            if elapsed_secs < 0:
                # Market hasn't started yet — block
                return

            sub_interval = int(elapsed_secs // self.market_interval)

            # Unique trade key: (market_start_timestamp, sub_interval)
            trade_key = (market_start_ts, sub_interval)

            # =========================================================================
            # TRADE WINDOW: minutes 3–4 of each 5-min market (180–240 seconds in)
            #
            # WHY LATE IN THE MARKET:
            #   At 3-4 minutes in, the UP/DOWN result is nearly decided. The price IS
            #   the trend — if YES is at $0.78, BTC went up during this interval.
            #   We're not predicting anymore, we're reading a nearly-resolved outcome.
            #
            # WHY NOT EARLIER (the old 30–90s window):
            #   At 30 seconds in, nobody knows which way BTC will move. The signals
            #   have no edge. This is why we were losing at prices near $0.50.
            #
            # TREND FILTER (applied in _make_trading_decision):
            #   Price > 0.60 → clear UP trend → buy YES
            #   Price < 0.40 → clear DOWN trend → buy NO
            #   Price 0.40–0.60 → coin flip → SKIP (don't trade)
            #
            # Share count intuition:
            #   1.4 shares = price $0.71 → strong trend, win rate ~71%
            #   1.9 shares = price $0.53 → weak trend, near coin flip
            #   2.0+ shares = price $0.50 → pure coin flip, SKIP
            # =========================================================================
            seconds_into_sub_interval = elapsed_secs % self.market_interval
            # Trade window: configurable % of interval (from .env)
            TRADE_WINDOW_START = int(self.market_interval * self.trade_window_start_pct)
            TRADE_WINDOW_END   = int(self.market_interval * self.trade_window_end_pct)

            if TRADE_WINDOW_START <= seconds_into_sub_interval < TRADE_WINDOW_END and trade_key != self.last_trade_time:
                self.last_trade_time = trade_key

                logger.info("=" * 80)
                logger.info(f" LATE-WINDOW TRADE: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                logger.info(f"   Market: {current_market['slug']}")
                logger.info(f"   Sub-interval #{sub_interval} ({seconds_into_sub_interval:.1f}s in = {seconds_into_sub_interval/60:.1f} min)")
                logger.info(f"   Price: ${float(mid_price):,.4f} | Bid: ${float(bid_decimal):,.4f} | Ask: ${float(ask_decimal):,.4f}")
                logger.info(f"   Trend strength: {'STRONG ✓' if float(mid_price) > 0.60 or float(mid_price) < 0.40 else 'WEAK — may skip'}")
                logger.info(f"   Price history: {len(self.price_history)} points")
                logger.info("=" * 80)

                self.run_in_executor(lambda: self._make_trading_decision_sync(float(mid_price)))

        except Exception as e:
            logger.error(f"Error processing quote tick: {e}")

    # ------------------------------------------------------------------
    # Trading decision (unchanged)
    # ------------------------------------------------------------------

    #def _make_trading_decision_sync(self, current_price):
    #    from decimal import Decimal
    #    price_decimal = Decimal(str(current_price))
    #    loop = asyncio.new_event_loop()
    #    asyncio.set_event_loop(loop)
    #    try:
    #        loop.run_until_complete(self._make_trading_decision(price_decimal))
    #    finally:
    #        loop.close()
    
    def _make_trading_decision_sync(self, current_price):
        """Synchronous wrapper for trading decision (called from executor)."""
        # Convert float back to Decimal for processing
        from decimal import Decimal
        price_decimal = Decimal(str(current_price))
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._make_trading_decision(price_decimal))
        finally:
            loop.close()
            
    async def _fetch_market_context(self, current_price: Decimal) -> dict:
        """
        Fetch REAL external data to populate signal processor metadata.

        Returns a dict with:
          - sentiment_score (float 0-100): live Fear & Greed index, or None
          - spot_price (float): live BTC-USD from Coinbase, or None
          - deviation (float): polymarket price vs SMA-20 (always computed)
          - momentum (float): 5-period rate of change (always computed)
          - volatility (float): price std-dev over last 20 ticks (always computed)
        """
        current_price_float = float(current_price)

        # --- Always-available stats from local price_history ---
        recent_prices = [float(p) for p in self.price_history[-20:]]
        sma_20 = sum(recent_prices) / len(recent_prices)
        deviation = (current_price_float - sma_20) / sma_20
        momentum = (
            (current_price_float - float(self.price_history[-5])) / float(self.price_history[-5])
            if len(self.price_history) >= 5 else 0.0
        )
        variance = sum((p - sma_20) ** 2 for p in recent_prices) / len(recent_prices)
        volatility = math.sqrt(variance)

        metadata = {
            "deviation": deviation,
            "momentum": momentum,
            "volatility": volatility,
            # Tick buffer for TickVelocityProcessor
            "tick_buffer": list(self._tick_buffer),
            # YES token id for OrderBookImbalanceProcessor
            "yes_token_id": self._yes_token_id,
        }

        # --- Real sentiment: Fear & Greed Index via NewsSocialDataSource ---
        try:
            from data_sources.news_social.adapter import NewsSocialDataSource
            news_source = NewsSocialDataSource()
            await news_source.connect()
            fg = await news_source.get_fear_greed_index()
            await news_source.disconnect()
            if fg and "value" in fg:
                metadata["sentiment_score"] = float(fg["value"])
                metadata["sentiment_classification"] = fg.get("classification", "")
                logger.info(
                    f"Fear & Greed: {metadata['sentiment_score']:.0f} "
                    f"({metadata['sentiment_classification']})"
                )
            else:
                logger.warning("Fear & Greed fetch returned no data — sentiment processor skipped")
        except Exception as e:
            logger.warning(f"Could not fetch Fear & Greed index: {e} — sentiment processor skipped")

        # --- Real spot price: Coinbase BTC-USD REST API ---
        try:
            from data_sources.coinbase.adapter import CoinbaseDataSource
            coinbase = CoinbaseDataSource()
            await coinbase.connect()
            spot = await coinbase.get_current_price()
            await coinbase.disconnect()
            if spot:
                metadata["spot_price"] = float(spot)
                logger.info(f"Coinbase spot price: ${float(spot):,.2f}")
            else:
                logger.warning("Coinbase price fetch returned None — divergence processor skipped")
        except Exception as e:
            logger.warning(f"Could not fetch Coinbase spot price: {e} — divergence processor skipped")

        logger.info(
            f"Market context — deviation={deviation:.2%}, "
            f"momentum={momentum:.2%}, volatility={volatility:.4f}, "
            f"sentiment={'%.0f' % metadata['sentiment_score'] if 'sentiment_score' in metadata else 'N/A'}, "
            f"spot=${'%.2f' % metadata['spot_price'] if 'spot_price' in metadata else 'N/A'}"
        )
        return metadata

    async def _make_trading_decision(self, current_price: Decimal):
        """
        Make trading decision using our 7-phase system.

        Position size is always $1.00 — no variable sizing, no risk-engine
        calculation needed. The risk engine is still used to check that we
        don't already have too many open positions.
        """
        # --- Mode check ---
        is_simulation = await self.check_simulation_mode()
        logger.info(f"Mode: {'SIMULATION' if is_simulation else 'LIVE TRADING'}")

        # --- Minimum history guard ---
        if len(self.price_history) < 20:
            logger.warning(f"Not enough price history ({len(self.price_history)}/20)")
            return

        logger.info(f"Current price: ${float(current_price):,.4f}")
        fused = None
        if self.enable_signals:
            # --- Phase 4a: Build real metadata for processors ---
            metadata = await self._fetch_market_context(current_price)

            # --- Phase 4b: Run all signal processors ---
            signals = self._process_signals(current_price, metadata)

            if not signals:
                logger.info("No signals generated — no trade this interval")
                return

            logger.info(f"Generated {len(signals)} signal(s):")
            for sig in signals:
                logger.info(
                    f"  [{sig.source}] {sig.direction.value}: "
                    f"score={sig.score:.1f}, confidence={sig.confidence:.2%}"
                )

            # --- Phase 4c: Fuse signals into one consensus ---
            # min_score lowered to 40 because the TREND FILTER (price at min 11-13)
            # is now the primary decision maker. Fusion is informational context,
            # not the trade gate. The trend gate below is the real filter.
            fused = self.fusion_engine.fuse_signals(signals, min_signals=1, min_score=40.0)
            if not fused:
                logger.info("Fusion produced no actionable signal — no trade this interval")
                return

            logger.info(
                f"FUSED SIGNAL: {fused.direction.value} "
                f"(score={fused.score:.1f}, confidence={fused.confidence:.2%})"
            )
        else:
            logger.info("Signals disabled — using trend-only mode")

        # --- Phase 5: Position size is always exactly $1.00 ---
        POSITION_SIZE_USD = Decimal("1.00")

        # =========================================================================
        # TREND FILTER — replaces signal-based direction at the late trade window
        #
        # At minute 13, the Polymarket price IS the market's verdict on BTC direction.
        # We ignore what the signal processors say and simply follow the price:
        #
        #   price > 0.60 → market says UP with >60% confidence → buy YES
        #   price < 0.40 → market says DOWN with >60% confidence → buy NO
        #   price 0.40–0.60 → too close to call → SKIP (this is where we were losing)
        #
        # This directly addresses the observation that trades at 1.9–2.0+ shares
        # (price near $0.50) almost always lose, while trades at 1.4 shares
        # (price ~$0.71) mostly win.
        # =========================================================================
        # Use configurable thresholds from .env
        price_float = float(current_price)

        if price_float > self.trend_up_threshold:
            direction = "long"
            trend_confidence = price_float  # e.g. 0.72 = 72% confident UP
            logger.info(
                f" TREND: UP ({price_float:.2%} YES probability) → buying YES (threshold: {self.trend_up_threshold:.2f})"
            )
        elif price_float < self.trend_down_threshold:
            direction = "short"
            trend_confidence = 1.0 - price_float  # e.g. 0.31 price = 69% confident DOWN
            logger.info(
                f" TREND: DOWN ({price_float:.2%} YES probability = {1-price_float:.2%} NO) → buying NO (threshold: {self.trend_down_threshold:.2f})"
            )
        else:
            logger.info(
                f"⏭ TREND: NEUTRAL ({price_float:.2%}) — price in uncertain zone, SKIPPING trade "
                f"(threshold zone: {self.trend_down_threshold:.0%}–{self.trend_up_threshold:.0%})"
            )
            self.last_trade_time = -1  # Allow retry on the next quote tick in this window
            return

        # Risk engine: only check position-count / exposure limits (no sizing math)
        is_valid, error = self.risk_engine.validate_new_position(
            size=POSITION_SIZE_USD,
            direction=direction,
            current_price=current_price,
        )
        if not is_valid:
            logger.warning(f"Risk engine blocked trade: {error}")
            return

        logger.info(f"Position size: $1.00 (fixed) | Direction: {direction.upper()}")

        # --- Liquidity guard: don't place if market has no real depth ---
        # The current bid/ask come from the last processed quote tick.
        # If ask <= 0.02 or bid <= 0.02, the orderbook is essentially empty
        # and a FAK (IOC market) order will be rejected immediately.
        last_tick = getattr(self, '_last_bid_ask', None)
        if last_tick:
            last_bid, last_ask = last_tick
            MIN_LIQUIDITY = Decimal("0.02")
            if direction == "long" and last_ask <= MIN_LIQUIDITY:
                logger.warning(
                    f"⚠ No liquidity for BUY: ask=${float(last_ask):.4f} ≤ {float(MIN_LIQUIDITY):.2f} — skipping trade, will retry next tick"
                )
                self.last_trade_time = -1  # Allow retry next tick
                return
            if direction == "short" and last_bid <= MIN_LIQUIDITY:
                logger.warning(
                    f"⚠ No liquidity for SELL: bid=${float(last_bid):.4f} ≤ {float(MIN_LIQUIDITY):.2f} — skipping trade, will retry next tick"
                )
                self.last_trade_time = -1  # Allow retry next tick
                return

        # --- Phase 5 / 6: Execute ---
        if is_simulation:
            # Call both: original for compatibility + new realtime tracking
            #await self._record_paper_trade(fused, POSITION_SIZE_USD, current_price, direction)
            # New realtime tracking
            await self._realtime_paper_trade(fused, POSITION_SIZE_USD, current_price, direction)
        else:
            await self._place_real_order(fused, POSITION_SIZE_USD, current_price, direction)
            
    async def _record_paper_trade(self, signal, position_size, current_price, direction):
        exit_delta = timedelta(minutes=1) if self.test_mode else timedelta(minutes=15)
        exit_time = datetime.now(timezone.utc) + exit_delta

        if "BULLISH" in str(signal.direction):
            movement = random.uniform(-0.02, 0.08)
        else:
            movement = random.uniform(-0.08, 0.02)

        exit_price = current_price * (Decimal("1.0") + Decimal(str(movement)))
        exit_price = max(Decimal("0.01"), min(Decimal("0.99"), exit_price))

        if direction == "long":
            pnl = position_size * (exit_price - current_price) / current_price
        else:
            pnl = position_size * (current_price - exit_price) / current_price

        outcome = "WIN" if pnl > 0 else "LOSS"
        paper_trade = PaperTrade(
            timestamp=datetime.now(timezone.utc),
            direction=direction.upper(),
            size_usd=float(position_size),
            price=float(current_price),
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            outcome=outcome,
        )
        self.paper_trades.append(paper_trade)

        self.performance_tracker.record_trade(
            trade_id=f"paper_{int(datetime.now().timestamp())}",
            direction=direction,
            entry_price=current_price,
            exit_price=exit_price,
            size=position_size,
            entry_time=datetime.now(timezone.utc),
            exit_time=exit_time,
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            metadata={
                "simulated": True,
                "num_signals": signal.num_signals if hasattr(signal, 'num_signals') else 1,
                "fusion_score": signal.score,
            }
        )

        if hasattr(self, 'grafana_exporter') and self.grafana_exporter:
            self.grafana_exporter.increment_trade_counter(won=(pnl > 0))
            self.grafana_exporter.record_trade_duration(exit_delta.total_seconds())

        logger.info("=" * 80)
        logger.info("[SIMULATION] PAPER TRADE RECORDED")
        logger.info(f"  Direction: {direction.upper()}")
        logger.info(f"  Size: ${float(position_size):.2f}")
        logger.info(f"  Entry Price: ${float(current_price):,.4f}")
        logger.info(f"  Simulated Exit: ${float(exit_price):,.4f}")
        logger.info(f"  Simulated P&L: ${float(pnl):+.2f} ({movement*100:+.2f}%)")
        logger.info(f"  Outcome: {outcome}")
        logger.info(f"  Total Paper Trades: {len(self.paper_trades)}")
        logger.info("=" * 80)

        self._save_paper_trades()

    # ------------------------------------------------------------------
    # Realtime Paper Trading (Enhanced)
    # ------------------------------------------------------------------

    async def _realtime_paper_trade(self, signal, position_size, current_price, direction):
        """Record real-time paper trade with accumulated UP/DOWN shares"""
        price_float = float(current_price)
        #usd_spent = float(position_size)
        usd_spent = price_float
        
        # Calculate actual shares bought: shares = USD / price
        #shares = usd_spent / price_float
        shares = 1.0
        
        # Determine market info
        if self.current_instrument_index >= 0 and self.current_instrument_index < len(self.all_btc_instruments):
            current_market = self.all_btc_instruments[self.current_instrument_index]
            market_slug = current_market.get('slug', 'unknown')
            market_start = current_market.get('start_time', datetime.now(timezone.utc))
            market_end = current_market.get('end_time', datetime.now(timezone.utc))
        else:
            market_slug = "unknown"
            market_start = datetime.now(timezone.utc)
            market_end = datetime.now(timezone.utc) + timedelta(seconds=self.market_interval)
        
        # Check if new market - reset accumulators
        if market_slug != self.current_market_slug:
            logger.info(f"[REALTIME PAPER] New market detected: {market_slug}, resetting accumulators")
            self.up_shares = 0.0
            self.up_usd_spent = 0.0
            self.down_shares = 0.0
            self.down_usd_spent = 0.0
            self.current_market_slug = market_slug
            self.current_market_tape = {
                "market_slug": market_slug,
                "market_start_time": market_start.isoformat(),
                "quotes": [],
            }
        
        # Accumulate shares based on direction
        if direction == "long":
            # Bought UP (YES)
            self.up_shares += shares
            self.up_usd_spent += usd_spent
            side_label = "UP (YES)"
            self.last_traded_direction = "UP"
        else:
            # Bought DOWN (NO)
            self.down_shares += shares
            usd_spent = 1 - usd_spent
            self.down_usd_spent += usd_spent
            side_label = "DOWN (NO)"
            self.last_traded_direction = "DOWN"
        
        # Print immediate entry log
        logger.info("=" * 80)
        logger.info("[REALTIME PAPER] TRADE OPENED")
        logger.info(f"  Direction: {side_label}")
        logger.info(f"  This trade: ${price_float:.4f} @ {shares:.4f} shares (${usd_spent:.2f})")
        logger.info(f"  Market: {market_slug}")
        logger.info(f"  Market Ends: {market_end.strftime('%H:%M:%S')}")
        logger.info("")
        logger.info(f"  [ACCUMULATED POSITION]")
        logger.info(f"    UP:   {self.up_shares:.4f} shares | ${self.up_usd_spent:.2f} spent")
        logger.info(f"    DOWN: {self.down_shares:.4f} shares | ${self.down_usd_spent:.2f} spent")
        total_spent = self.up_usd_spent + self.down_usd_spent
        logger.info(f"    TOTAL: ${total_spent:.2f} spent")
        logger.info("=" * 80)
        
        # Also call original for compatibility
        #await self._record_paper_trade(signal, position_size, current_price, direction)

    def _close_realtime_position_sync(self, market_slug, up_shares, up_usd, down_shares, down_usd):
        """Close accumulated realtime positions - sync with polling"""
        logger.info("=" * 80)
        logger.info("[REALTIME PAPER] MARKET CLOSED - FINAL POSITION")
        logger.info(f"  Market: {market_slug}")
        logger.info(f"  UP shares: {up_shares:.4f} | USD spent: ${up_usd:.2f}")
        logger.info(f"  DOWN shares: {down_shares:.4f} | USD spent: ${down_usd:.2f}")
        logger.info("")
        
        total_spent = up_usd + down_usd
        logger.info(f"  Total USD spent: ${total_spent:.2f}")
        
        # Determine which direction bet
        if up_shares > down_shares:
            logger.info(f"  Direction bet: UP (more UP shares)")
        elif down_shares > up_shares:
            logger.info(f"  Direction bet: DOWN (more DOWN shares)")
        else:
            logger.info(f"  Direction bet: EQUAL")
        
        logger.info("")
        
        # Poll for resolution synchronously (max 90 seconds)
        max_retries = 9
        retry_delay = 10
        logger.info(f"  Polling for resolution (max {max_retries} tries)...")
        
        winner = "UNKNOWN"
        for attempt in range(max_retries):
            winner = get_market_winner_sync(market_slug)
            if winner != "UNKNOWN":
                logger.info(f"  Resolution found on attempt {attempt + 1}: {winner}")
                break
            if attempt < max_retries - 1:
                logger.info(f"  Attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
        
        if winner == "UNKNOWN":
            logger.warning(f"  Could not resolve after {max_retries} attempts")
        
        logger.info(f"  Market winner: {winner}")
        
        if winner == "YES":
            logger.info(f"  Actual outcome: UP WON")
        elif winner == "NO":
            logger.info(f"  Actual outcome: DOWN WON")
        else:
            logger.info(f"  Actual outcome: {winner}")
        
        logger.info("=" * 80)
        
        # Write to close_market.log with flush
        logger.info(f"  Writing to close_market.log (winner={winner})...")
        try:
            with open('close_market.log', 'a') as f:
                f.write(f"[{datetime.now(timezone.utc).isoformat()}] Market: {market_slug}\n")
                f.write(f"  UP: {up_shares:.4f} shares | ${up_usd:.2f}\n")
                f.write(f"  DOWN: {down_shares:.4f} shares | ${down_usd:.2f}\n")
                f.write(f"  Winner: {winner}\n")
                f.write("\n")
                f.flush()
            logger.info("  Log written successfully")
        except Exception as e:
            logger.warning(f"Failed to write close_market.log: {e}")
        
        # Clear current position
        self.current_realtime_position = None

    def _poll_and_write_resolution(self):
        """Poll for previous market resolution and write to log"""
        market_slug = self.last_market_slug
        up_shares = self.last_up_shares
        up_usd = self.last_up_usd
        down_shares = self.last_down_shares
        down_usd = self.last_down_usd
        market_tape = self.last_market_tape.copy() if self.last_market_tape else {
            "market_slug": market_slug,
            "market_start_time": None,
            "quotes": [],
        }
        
        logger.info("=" * 80)
        logger.info("[REALTIME PAPER] CHECKING PREVIOUS MARKET RESOLUTION")
        logger.info(f"  Market: {market_slug}")
        logger.info(f"  UP: {up_shares:.4f} shares | ${up_usd:.2f}")
        logger.info(f"  DOWN: {down_shares:.4f} shares | ${down_usd:.2f}")
        
        # Poll for resolution
        max_retries = 9
        retry_delay = 10
        logger.info(f"  Polling for resolution (max {max_retries} tries)...")
        logger.info(f"  Last known price: {self.last_market_price}")
        
        winner = "UNKNOWN"
        for attempt in range(max_retries):
            winner = get_market_winner_sync(market_slug, self.last_market_price)
            if winner != "UNKNOWN":
                logger.info(f"  Resolution found on attempt {attempt + 1}: {winner}")
                break
            if attempt < max_retries - 1:
                logger.info(f"  Attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
        
        if winner == "UNKNOWN":
            logger.warning(f"  Could not resolve after {max_retries} attempts")
        
        logger.info(f"  Market winner: {winner}")
        logger.info("=" * 80)
        market_tape["winner"] = winner
        
        # Write to log
        logger.info(f"  Writing to close_market.log...")
        try:
            with open('close_market.log', 'a') as f:
                f.write(f"[{datetime.now(timezone.utc).isoformat()}] Market: {market_slug}\n")
                f.write(f"  UP: {up_shares:.4f} shares | ${up_usd:.2f}\n")
                f.write(f"  DOWN: {down_shares:.4f} shares | ${down_usd:.2f}\n")
                f.write(f"  Winner: {winner}\n")
                f.write("\n")
                f.flush()
            logger.info("  Log written successfully")
        except Exception as e:
            logger.warning(f"Failed to write close_market.log: {e}")

        try:
            with open(self.market_tape_path, 'a') as f:
                f.write(json.dumps(market_tape) + "\n")
                f.flush()
            logger.info(f"  Market tape written to {self.market_tape_path}")
        except Exception as e:
            logger.warning(f"Failed to write market tape: {e}")
        
        # Clear last market info
        self.last_market_slug = ""
        self.last_market_tape = {}

    def _save_paper_trades(self):
        import json
        try:
            trades_data = [t.to_dict() for t in self.paper_trades]
            with open('paper_trades.json', 'w') as f:
                json.dump(trades_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")

    # ------------------------------------------------------------------
    # Real order (unchanged)
    # ------------------------------------------------------------------

    async def _place_real_order(self, signal, position_size, current_price, direction):
        if not self.instrument_id:
            logger.error("No instrument available")
            return

        try:
            # instrument is fetched below after determining YES vs NO token

            logger.info("=" * 80)
            logger.info("LIVE MODE - PLACING REAL ORDER!")
            logger.info("=" * 80)

            # On Polymarket, both UP and DOWN are BUY orders.
            # Bullish = buy YES token (self._yes_instrument_id)
            # Bearish = buy NO token  (self._no_instrument_id)
            # There is NO sell — you always buy whichever side you want.
            side = OrderSide.BUY

            if direction == "long":
                trade_instrument_id = getattr(self, '_yes_instrument_id', self.instrument_id)
                trade_label = "YES (UP)"
            else:
                no_id = getattr(self, '_no_instrument_id', None)
                if no_id is None:
                    logger.warning(
                        "NO token instrument not found for this market — "
                        "cannot bet DOWN. Skipping trade."
                    )
                    return
                trade_instrument_id = no_id
                trade_label = "NO (DOWN)"

            instrument = self.cache.instrument(trade_instrument_id)
            if not instrument:
                logger.error(f"Instrument not in cache: {trade_instrument_id}")
                return

            logger.info(f"Buying {trade_label} token: {trade_instrument_id}")

            trade_price = float(current_price)
            max_usd_amount = float(position_size)

            precision = instrument.size_precision

            # Always BUY — the market-order patch converts this to a USD amount.
            # Pass dummy qty=5 (minimum) so Nautilus risk engine doesn't deny it.
            MIN_QTY = 1.0
            min_qty_val = float(getattr(instrument, 'min_quantity', None) or MIN_QTY)
            token_qty = max(min_qty_val, MIN_QTY)
            token_qty = round(token_qty, precision)
            logger.info(
                f"BUY {trade_label}: dummy qty={token_qty:.6f} "
                f"(patch converts to ${max_usd_amount:.2f} USD)"
            )

            qty = Quantity(token_qty, precision=precision)
            timestamp_ms = int(time.time() * 1000)
            unique_id = f"BTC-XMIN-${max_usd_amount:.0f}-{timestamp_ms}"

            order = self.order_factory.market(
                instrument_id=trade_instrument_id,
                order_side=side,
                quantity=qty,
                client_order_id=ClientOrderId(unique_id),
                quote_quantity=False,
                time_in_force=TimeInForce.IOC,
            )

            self.submit_order(order)

            logger.info(f"REAL ORDER SUBMITTED!")
            logger.info(f"  Order ID: {unique_id}")
            logger.info(f"  Direction: {trade_label}")
            logger.info(f"  Side: BUY")
            logger.info(f"  Token Quantity: {token_qty:.6f}")
            logger.info(f"  Estimated Cost: ~${max_usd_amount:.2f}")
            logger.info(f"  Price: ${trade_price:.4f}")
            logger.info("=" * 80)

            self._track_order_event("placed")

        except Exception as e:
            logger.error(f"Error placing real order: {e}")
            import traceback
            traceback.print_exc()
            self._track_order_event("rejected")

    # ------------------------------------------------------------------
    # Signal processing
    # ------------------------------------------------------------------

    def _process_signals(self, current_price, metadata=None):
        signals = []
        if metadata is None:
            metadata = {}

        processed_metadata = {}
        for key, value in metadata.items():
            if isinstance(value, float):
                processed_metadata[key] = Decimal(str(value))
            else:
                processed_metadata[key] = value

        spike_signal = self.spike_detector.process(
            current_price=current_price,
            historical_prices=self.price_history,
            metadata=processed_metadata,
        )
        if spike_signal:
            signals.append(spike_signal)

        if 'sentiment_score' in processed_metadata:
            sentiment_signal = self.sentiment_processor.process(
                current_price=current_price,
                historical_prices=self.price_history,
                metadata=processed_metadata,
            )
            if sentiment_signal:
                signals.append(sentiment_signal)

        if 'spot_price' in processed_metadata:
            divergence_signal = self.divergence_processor.process(
                current_price=current_price,
                historical_prices=self.price_history,
                metadata=processed_metadata,
            )
            if divergence_signal:
                signals.append(divergence_signal)

        # --- Order Book Imbalance (real-time Polymarket CLOB depth) ---
        if processed_metadata.get('yes_token_id'):
            ob_signal = self.orderbook_processor.process(
                current_price=current_price,
                historical_prices=self.price_history,
                metadata=processed_metadata,
            )
            if ob_signal:
                signals.append(ob_signal)

        # --- Tick Velocity (last 60s of Polymarket probability movement) ---
        if processed_metadata.get('tick_buffer'):
            tv_signal = self.tick_velocity_processor.process(
                current_price=current_price,
                historical_prices=self.price_history,
                metadata=processed_metadata,
            )
            if tv_signal:
                signals.append(tv_signal)

        # --- Deribit Put/Call Ratio (institutional options sentiment) ---
        pcr_signal = self.deribit_pcr_processor.process(
            current_price=current_price,
            historical_prices=self.price_history,
            metadata=processed_metadata,
        )
        if pcr_signal:
            signals.append(pcr_signal)

        return signals

    # ------------------------------------------------------------------
    # Order events
    # ------------------------------------------------------------------

    def _track_order_event(self, event_type: str) -> None:
        """
        Safely track an order event on the performance tracker.

        PerformanceTracker does not expose `increment_order_counter`, so we
        use whichever method is actually available, or fall back to a no-op.
        Supported event_type values: "placed", "filled", "rejected".
        """
        try:
            pt = self.performance_tracker
            # Try the method that actually exists first
            if hasattr(pt, 'record_order_event'):
                pt.record_order_event(event_type)
            elif hasattr(pt, 'increment_counter'):
                pt.increment_counter(event_type)
            elif hasattr(pt, 'increment_order_counter'):
                pt.increment_order_counter(event_type)
            else:
                # No suitable method found – log and carry on
                logger.debug(
                    f"PerformanceTracker has no order-counter method; "
                    f"ignoring event '{event_type}'"
                )
        except Exception as e:
            logger.warning(f"Failed to track order event '{event_type}': {e}")

    def on_order_filled(self, event):
        logger.info("=" * 80)
        logger.info(f"ORDER FILLED!")
        logger.info(f"  Order: {event.client_order_id}")
        logger.info(f"  Fill Price: ${float(event.last_px):.4f}")
        logger.info(f"  Quantity: {float(event.last_qty):.6f}")
        logger.info("=" * 80)
        self._track_order_event("filled")

    def on_order_denied(self, event):
        logger.error("=" * 80)
        logger.error(f"ORDER DENIED!")
        logger.error(f"  Order: {event.client_order_id}")
        logger.error(f"  Reason: {event.reason}")
        logger.error("=" * 80)
        self._track_order_event("rejected")

    def on_order_rejected(self, event):
        """Handle order rejection — reset trade timer so we can retry next tick."""
        reason = str(getattr(event, 'reason', ''))
        reason_lower = reason.lower()
        if 'no orders found' in reason_lower or 'fak' in reason_lower or 'no match' in reason_lower:
            logger.warning(
                f"⚠ FAK rejected (no liquidity) — resetting timer to retry next tick\n"
                f"  Reason: {reason}"
            )
            self.last_trade_time = -1  # Allow retry on next quote tick
        else:
            logger.warning(f"Order rejected: {reason}")

    # ------------------------------------------------------------------
    # Grafana / stop
    # ------------------------------------------------------------------

    def _start_grafana_sync(self):
        import asyncio
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.grafana_exporter.start())
            logger.info("Grafana metrics started on port 8000")
        except Exception as e:
            logger.error(f"Failed to start Grafana: {e}")

    def on_stop(self):
        logger.info("Integrated BTC strategy stopped")
        logger.info(f"Total paper trades recorded: {len(self.paper_trades)}")
        if self.grafana_exporter:
            import asyncio
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(self.grafana_exporter.stop())
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_integrated_bot(simulation: bool = False, enable_grafana: bool = True, test_mode: bool = False, market_interval: int = DEFAULT_MARKET_INTERVAL):
    """Run the integrated BTC trading bot - LOADS ALL BTC MARKETS FOR THE DAY"""
    
    interval_min = market_interval // 60
    interval_label = f"{interval_min}-MIN"
    
    print("=" * 80)
    print(f"INTEGRATED POLYMARKET BTC {interval_label} TRADING BOT")
    print("Nautilus + 7-Phase System + Redis Control")
    print("=" * 80)

    redis_client = init_redis()

    if redis_client:
        try:
            # ALWAYS overwrite Redis with the current session mode.
            # This prevents a stale value from a previous --live run
            # silently overriding --test-mode or --simulation runs.
            mode_value = '1' if simulation else '0'
            redis_client.set('btc_trading:simulation_mode', mode_value)
            mode_label = 'SIMULATION' if simulation else 'LIVE'
            logger.info(f"Redis simulation_mode forced to: {mode_label} ({mode_value})")
        except Exception as e:
            logger.warning(f"Could not set Redis simulation mode: {e}")

    print(f"\nConfiguration:")
    thresholds = get_strategy_thresholds()
    print(f"  Initial Mode: {'SIMULATION' if simulation else 'LIVE TRADING'}")
    print(f"  Redis Control: {'Enabled' if redis_client else 'Disabled'}")
    print(f"  Grafana: {'Enabled' if enable_grafana else 'Disabled'}")
    print(f"  Max Trade Size: ${os.getenv('MARKET_BUY_USD', '1.00')}")
    print(f"  Quote stability gate: {QUOTE_STABILITY_REQUIRED} valid ticks")
    print(f"  Market Interval: {interval_min} minutes")
    print(f"  Trade Window: {int(thresholds['trade_window_start_pct']*100)}%-{int(thresholds['trade_window_end_pct']*100)}% of interval")
    print(f"  Price Thresholds: UP>{thresholds['trend_up_threshold']:.2f} | DOWN<{thresholds['trend_down_threshold']:.2f}")
    print()

    now = datetime.now(timezone.utc)
    
    # =========================================================================
    # Slug timestamps ARE standard Unix timestamps (no offset) aligned to
    # configurable boundaries. Generate slugs for current + next 24 hours.
    # =========================================================================
    now = datetime.now(timezone.utc)
    unix_interval_start = (int(now.timestamp()) // market_interval) * market_interval

    btc_slugs = []
    for i in range(-1, 97):  # include 1 prior interval (in case we're just after boundary)
        timestamp = unix_interval_start + (i * market_interval)
        btc_slugs.append(f"btc-updown-{interval_min}m-{timestamp}")

    logger.info("=" * 80)
    logger.info(f"LOADING BTC {interval_min}-MIN MARKETS BY SLUG")
    logger.info(f"  Interval start: {unix_interval_start} | Count: {len(btc_slugs)}")
    logger.info(f"  First: {btc_slugs[0]}  Last: {btc_slugs[-1]}")
    logger.info("=" * 80)

    _, poly_data_cfg, poly_exec_cfg = build_polymarket_client_configs(btc_slugs, signature_type=0)

    config = TradingNodeConfig(
        environment="live",
        trader_id=f"BTC-{interval_label}-INTEGRATED-001",
        logging=LoggingConfig(
            log_level="INFO",
            log_directory="./logs/nautilus",
        ),
        data_engine=LiveDataEngineConfig(qsize=6000),
        exec_engine=LiveExecEngineConfig(qsize=6000),
        risk_engine=LiveRiskEngineConfig(bypass=simulation),
        data_clients={POLYMARKET: poly_data_cfg},
        exec_clients={POLYMARKET: poly_exec_cfg},
    )

    strategy = IntegratedBTCStrategy(
        redis_client=redis_client,
        enable_grafana=enable_grafana,
        test_mode=test_mode,
        market_interval=market_interval,
        thresholds=get_strategy_thresholds(),
    )

    print("\nBuilding Nautilus node...")
    node = TradingNode(config=config)
    node.add_data_client_factory(POLYMARKET, PolymarketLiveDataClientFactory)
    node.add_exec_client_factory(POLYMARKET, PolymarketLiveExecClientFactory)
    node.trader.add_strategy(strategy)
    node.build()
    logger.info("Nautilus node built successfully")

    print()
    print("=" * 80)
    print("BOT STARTING")
    print("=" * 80)

    try:
        node.run()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        node.dispose()
        logger.info("Bot stopped")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Integrated BTC Trading Bot")
    parser.add_argument("--live", action="store_true",
                        help="Run in LIVE mode (real money at risk!). Default is simulation.")
    parser.add_argument("--no-grafana", action="store_true", help="Disable Grafana metrics")
    parser.add_argument("--test-mode", action="store_true",
                        help="Run in TEST MODE (trade every minute for faster testing)")
    parser.add_argument("--interval", type=int, default=DEFAULT_MARKET_INTERVAL,
                        help=f"Market interval in seconds (default: {DEFAULT_MARKET_INTERVAL}, use 900 for 15-min)")

    args = parser.parse_args()
    enable_grafana = not args.no_grafana
    test_mode = args.test_mode
    market_interval = args.interval

    # Validate interval
    if market_interval not in (300, 900):
        logger.warning(f"Unusual interval {market_interval}s, supported values: 300 (5min), 900 (15min)")

    # --test-mode ALWAYS forces simulation even if --live is also passed
    if args.test_mode:
        simulation = True
    else:
        simulation = not args.live

    if not simulation:
        logger.warning("=" * 80)
        logger.warning("LIVE TRADING MODE — REAL MONEY AT RISK!")
        logger.warning("=" * 80)
    else:
        logger.info("=" * 80)
        logger.info(f"SIMULATION MODE — {'TEST MODE (fast clock)' if test_mode else 'paper trading only'}")
        logger.info("No real orders will be placed.")
        logger.info("=" * 80)

    run_integrated_bot(simulation=simulation, enable_grafana=enable_grafana, test_mode=test_mode, market_interval=market_interval)


if __name__ == "__main__":
    main()
