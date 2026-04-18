import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeSticker,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
)
from tqdm import tqdm

load_dotenv()

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")


class RateLimiter:
    """Rate limiter to restrict API calls to max requests per second."""

    def __init__(self, max_requests_per_second: int = 5):
        self.max_requests = max_requests_per_second
        self.timestamps: list[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            # Remove timestamps older than 1 second
            self.timestamps = [t for t in self.timestamps if now - t < 1.0]

            if len(self.timestamps) >= self.max_requests:
                # Wait until the oldest request is more than 1 second old
                sleep_time = 1.0 - (now - self.timestamps[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                self.timestamps = self.timestamps[1:]

            self.timestamps.append(time.time())


rate_limiter = RateLimiter(max_requests_per_second=5)


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


def save_json(path: Path, data: dict | list):
    """Save data as JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


async def fetch_channel_info(client: TelegramClient, channel: str) -> dict:
    """Fetch channel metadata."""
    # await rate_limiter.acquire()
    entity = await client.get_entity(channel)
    return serialize_value(entity.to_dict())


async def download_post_media(
    client: TelegramClient, message, post_dir: Path
) -> list[str]:
    """Download all media from a message. Returns list of downloaded filenames."""
    downloaded = []

    if message.media is None:
        return downloaded

    if isinstance(message.media, MessageMediaPhoto):
        # await rate_limiter.acquire()
        filename = f"photo_{message.id}.jpg"
        filepath = post_dir / filename
        await client.download_media(message, filepath)
        downloaded.append(filename)

    elif isinstance(message.media, MessageMediaDocument):
        doc = message.media.document
        if doc is None:
            return downloaded

        # Skip files larger than 20MB
        max_size = 20 * 1024 * 1024  # 20MB
        if doc.size and doc.size > max_size:
            return downloaded

        # Only download voice messages, round videos, and stickers
        filename = None
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeAudio) and attr.voice:
                filename = "voice.ogg"
                break
            elif isinstance(attr, DocumentAttributeVideo) and attr.round_message:
                filename = "video_note.mp4"
                break
            elif isinstance(attr, DocumentAttributeSticker):
                ext = doc.mime_type.split("/")[-1] if doc.mime_type else "webp"
                filename = f"sticker.{ext}"
                break

        # Skip if not voice or video note
        if filename is None:
            return downloaded

        filepath = post_dir / filename
        await client.download_media(message, filepath)
        downloaded.append(filename)

    return downloaded


async def fetch_and_save_posts(
    client: TelegramClient,
    channel: str,
    data_dir: Path,
    limit: int | None = None,
    since: datetime | None = None,
):
    """Fetch posts and save them with media."""
    posts_dir = data_dir / "posts"
    posts_dir.mkdir(parents=True, exist_ok=True)

    # Get total message count for progress bar
    # await rate_limiter.acquire()
    total = (await client.get_messages(channel, limit=0)).total
    if limit:
        total = min(total, limit)

    all_posts = []

    with tqdm(total=total, desc="Fetching posts", unit="post") as pbar:
        # await rate_limiter.acquire()
        async for message in client.iter_messages(channel, limit=limit):
            # Stop if message is older than since date
            if since and message.date and message.date.replace(tzinfo=None) < since:
                break

            post_data = serialize_value(message.to_dict())
            all_posts.append(post_data)

            post_id = str(message.id)
            post_dir = posts_dir / post_id

            # Check if post directory exists and is not empty
            if post_dir.exists() and any(post_dir.iterdir()):
                pbar.update(1)
                continue

            # Create post directory and save post info
            post_dir.mkdir(parents=True, exist_ok=True)
            save_json(post_dir / "post.json", post_data)

            # Download media
            await download_post_media(client, message, post_dir)

            pbar.update(1)

    # Save all posts summary
    save_json(posts_dir / "posts.json", all_posts)

    return all_posts


async def main():
    parser = argparse.ArgumentParser(description="Fetch posts from a Telegram channel")
    parser.add_argument("channel", help="Channel username or link")
    parser.add_argument(
        "--limit", "-l", type=int, default=None, help="Limit to last N posts"
    )
    parser.add_argument(
        "--since",
        "-s",
        type=str,
        default=None,
        help="Only fetch posts after this date (yyyy-mm-dd)",
    )
    args = parser.parse_args()

    since_date = None
    if args.since:
        since_date = datetime.strptime(args.since, "%Y-%m-%d")

    if not API_ID or not API_HASH:
        print("Error: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set")
        sys.exit(1)

    channel = args.channel
    data_dir = Path("data") / channel.lstrip("@").split("/")[-1]
    data_dir.mkdir(parents=True, exist_ok=True)

    async with TelegramClient("session", int(API_ID), API_HASH) as client:
        # Fetch and save channel info
        print(f"Fetching channel info for {channel}...")
        channel_info = await fetch_channel_info(client, channel)
        save_json(data_dir / "channel_info.json", channel_info)

        # Fetch and save posts
        print(f"Fetching posts from {channel}...")
        posts = await fetch_and_save_posts(
            client, channel, data_dir, args.limit, since_date
        )
        print(f"Saved {len(posts)} posts to {data_dir}")


if __name__ == "__main__":
    asyncio.run(main())
