from polymarket_node_config import build_polymarket_client_configs


def test_build_polymarket_client_configs_uses_instrument_config():
    instrument_config, data_cfg, exec_cfg = build_polymarket_client_configs(
        ["btc-updown-15m-1778999400"]
    )

    assert instrument_config.load_all is True
    assert instrument_config.use_gamma_markets is True
    assert instrument_config.filters["slug"] == ("btc-updown-15m-1778999400",)
    assert data_cfg.instrument_config is instrument_config
    assert exec_cfg.instrument_config is instrument_config
    assert data_cfg.signature_type == 3
    assert exec_cfg.signature_type == 3


def test_build_polymarket_client_configs_forwards_funder(monkeypatch):
    monkeypatch.setenv("POLYMARKET_FUNDER", "0x1234567890abcdef1234567890abcdef12345678")

    _, _, exec_cfg = build_polymarket_client_configs(["btc-updown-15m-1778999400"])

    assert exec_cfg.funder == "0x1234567890abcdef1234567890abcdef12345678"
