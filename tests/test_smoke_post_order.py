from smoke_post_order import format_market_summary


def test_format_market_summary_is_readable() -> None:
    summary = format_market_summary("slug", "123", "0.55")
    assert summary == "slug=slug token_id=123 outcome_price=0.55"
