from rotate_api_key import format_creds_output


class DummyCreds:
    api_key = "k"
    api_secret = "s"
    api_passphrase = "p"


def test_format_creds_output_prints_env_lines() -> None:
    output = format_creds_output(DummyCreds())
    assert "POLYMARKET_API_KEY=k" in output
    assert "POLYMARKET_API_SECRET=s" in output
    assert "POLYMARKET_PASSPHRASE=p" in output
