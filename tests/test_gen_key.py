from gen_key import format_creds_output


class DummyCreds:
    api_key = "key123"
    api_secret = "secret123"
    api_passphrase = "pass123"


def test_format_creds_output_uses_env_style_labels() -> None:
    output = format_creds_output(DummyCreds())
    assert "POLYMARKET_API_KEY=key123" in output
    assert "POLYMARKET_API_SECRET=secret123" in output
    assert "POLYMARKET_PASSPHRASE=pass123" in output
