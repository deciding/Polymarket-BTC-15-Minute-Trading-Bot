from check_polymarket_balance import format_balance_output


def test_format_balance_output_uses_usdc_only() -> None:
    output = format_balance_output({"USDC": 12.3456})
    assert output == "USDC balance: $12.35"
