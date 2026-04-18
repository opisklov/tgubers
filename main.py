import asyncio
import json
import os
import sys
from tqdm import tqdm
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")


def serialize_value(obj):
    """Recursively serialize Telethon objects to JSON-compatible format."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (list, tuple)):
        return [serialize_value(item) for item in obj]
    if isinstance(obj, dict):
        return {k: serialize_value(v) for k, v in obj.items()}
    if hasattr(obj, "to_dict"):
        return serialize_value(obj.to_dict())
    return str(obj)


async def fetch_channel_posts(client: TelegramClient, channel: str) -> list[dict]:
    # Get total message count for progress bar
    total = (await client.get_messages(channel, limit=0)).total

    posts = []
    with tqdm(total=total, desc="Fetching posts", unit="post") as pbar:
        async for message in client.iter_messages(channel):
            post = serialize_value(message.to_dict())
            posts.append(post)
            pbar.update(1)
    return posts


def save_posts(channel: str, posts: list[dict]) -> Path:
    data_dir = Path("data") / channel
    data_dir.mkdir(parents=True, exist_ok=True)

    output_file = data_dir / "posts.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(posts, f, ensure_ascii=False, indent=2)

    return output_file


async def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <channel_username>")
        sys.exit(1)

    channel = sys.argv[1]

    if not API_ID or not API_HASH:
        print("Error: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set")
        sys.exit(1)

    async with TelegramClient("session", int(API_ID), API_HASH) as client:
        print(f"Fetching posts from {channel}...")
        posts = await fetch_channel_posts(client, channel)
        output_file = save_posts(channel, posts)
        print(f"Saved {len(posts)} posts to {output_file}")


async def test():
    async with TelegramClient("session", API_ID, API_HASH) as client:
        me = await client.get_me()
        print(me.stringify())

if __name__ == "__main__":
    asyncio.run(main())
