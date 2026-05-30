from check_nautilus_balance import format_balance_output


def test_format_balance_output_includes_usdc_fields() -> None:
    output = format_balance_output({"USDC": 12.34, "free": 10.0, "locked": 2.34})
    assert output == "USDC total: $12.34 | free: $10.00 | locked: $2.34"
