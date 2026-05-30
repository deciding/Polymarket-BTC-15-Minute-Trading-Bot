import os

from dotenv import load_dotenv
from py_clob_client_v2 import ClobClient
from py_clob_client_v2.clob_types import ApiCreds


def format_creds_output(creds) -> str:
    return "\n".join(
        [
            "--- Updated .env values ---",
            f"POLYMARKET_API_KEY={creds.api_key}",
            f"POLYMARKET_API_SECRET={creds.api_secret}",
            f"POLYMARKET_PASSPHRASE={creds.api_passphrase}",
        ]
    )


def main() -> int:
    load_dotenv('.env')

    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=os.getenv("POLYMARKET_PK"),
        signature_type=3,
        funder=os.getenv("POLYMARKET_FUNDER"),
    )

    derived = client.derive_api_key()
    print("Current derived API key:")
    print(format_creds_output(derived))

    client.set_api_creds(derived)
    keys_before = client.get_api_keys()
    print(f"Current API keys: {keys_before}")

    client.delete_api_key()
    print("Deleted current API key")

    fresh = client.create_api_key()
    print("Created fresh API key:")
    print(format_creds_output(fresh))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
