import sys
from pathlib import Path


def parse_close_market_log(text: str):
    blocks = []
    skipped = 0
    current = {}

    def flush_current() -> None:
        nonlocal current, skipped
        if not current:
            return
        if current.get("winner") == "UNKNOWN":
            skipped += 1
        else:
            blocks.append(current)
        current = {}

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("[") and "Market:" in line:
            flush_current()
            current = {"market_slug": line.split("Market:", 1)[1].strip()}
        elif line.startswith("UP:"):
            parts = line.split()
            current["up_shares"] = float(parts[1])
            current["up_usd"] = float(parts[4].replace("$", ""))
        elif line.startswith("DOWN:"):
            parts = line.split()
            current["down_shares"] = float(parts[1])
            current["down_usd"] = float(parts[4].replace("$", ""))
        elif line.startswith("Winner:"):
            current["winner"] = line.split(":", 1)[1].strip().upper()

    flush_current()
    return blocks, skipped


def summarize_blocks(blocks):
    usd_spent = 0.0
    earnings = 0.0

    for block in blocks:
        spent = float(block["up_usd"]) + float(block["down_usd"])
        usd_spent += spent
        if block["winner"] == "UP":
            earnings += float(block["up_shares"])
        elif block["winner"] == "DOWN":
            earnings += float(block["down_shares"])

    usd_spent = round(usd_spent, 2)
    earnings = round(earnings, 2)
    net_profit = round(earnings - usd_spent, 2)

    return {
        "markets": len(blocks),
        "usd_spent": usd_spent,
        "earnings": earnings,
        "net_profit": net_profit,
    }


def main(argv):
    path = Path(argv[1]) if len(argv) > 1 else Path("close_market.log")
    text = path.read_text()
    blocks, skipped = parse_close_market_log(text)
    summary = summarize_blocks(blocks)

    print(f"Markets processed: {summary['markets']}")
    print(f"Skipped UNKNOWN blocks: {skipped}")
    print(f"Total USD spent: ${summary['usd_spent']:.2f}")
    print(f"Total earnings: ${summary['earnings']:.2f}")
    print(f"Net profit: ${summary['net_profit']:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
