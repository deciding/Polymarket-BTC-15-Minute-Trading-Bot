from check_clob_balance import format_allowance_output


def test_format_allowance_output_contains_balance_and_allowance() -> None:
    output = format_allowance_output({"balance": "1.5", "allowances": {"default": "2.0"}})
    assert output == "balance=1.5 | allowances={'default': '2.0'}"
