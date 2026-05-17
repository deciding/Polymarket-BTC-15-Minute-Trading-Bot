from summarize_close_markets import parse_close_market_log, summarize_blocks


def test_parse_close_market_log_skips_unknown_blocks() -> None:
    text = """[2026-05-17T12:55:01.499316+00:00] Market: btc-updown-5m-1779022200
  UP: 1.0000 shares | $0.81
  DOWN: 0.0000 shares | $0.00
  Winner: DOWN

[2026-05-17T13:00:01.861268+00:00] Market: btc-updown-5m-1779022500
  UP: 0.0000 shares | $0.00
  DOWN: 1.0000 shares | $0.94
  Winner: UNKNOWN
"""

    blocks, skipped = parse_close_market_log(text)

    assert len(blocks) == 1
    assert skipped == 1
    assert blocks[0]["market_slug"] == "btc-updown-5m-1779022200"
    assert blocks[0]["winner"] == "DOWN"


def test_summarize_blocks_computes_spent_earnings_and_profit() -> None:
    blocks = [
        {
            "market_slug": "m1",
            "up_shares": 1.0,
            "up_usd": 0.81,
            "down_shares": 0.0,
            "down_usd": 0.0,
            "winner": "DOWN",
        },
        {
            "market_slug": "m2",
            "up_shares": 0.0,
            "up_usd": 0.0,
            "down_shares": 1.0,
            "down_usd": 0.94,
            "winner": "DOWN",
        },
    ]

    summary = summarize_blocks(blocks)

    assert summary["markets"] == 2
    assert summary["usd_spent"] == 1.75
    assert summary["earnings"] == 1.0
    assert summary["net_profit"] == -0.75
