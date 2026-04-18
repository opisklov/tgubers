import argparse
import json
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import numbers


def get_media_type(media: dict | None) -> str:
    """Extract media type from media object."""
    if media is None:
        return "text"

    media_class = media.get("_", "text")

    if media_class == "MessageMediaPhoto":
        return "photo"
    elif media_class == "MessageMediaDocument":
        doc = media.get("document", {})
        attributes = doc.get("attributes", [])

        for attr in attributes:
            attr_type = attr.get("_", "")
            if attr_type == "DocumentAttributeAudio":
                if attr.get("voice"):
                    return "voice"
                return "audio"
            elif attr_type == "DocumentAttributeVideo":
                if attr.get("round_message"):
                    return "video_note"
                return "video"
            elif attr_type == "DocumentAttributeSticker":
                return "sticker"
            elif attr_type == "DocumentAttributeAnimated":
                return "animation"

        return "document"
    elif media_class == "MessageMediaWebPage":
        return "webpage"
    elif media_class == "MessageMediaPoll":
        return "poll"
    elif media_class == "MessageMediaGeo":
        return "geo"
    elif media_class == "MessageMediaContact":
        return "contact"

    return media_class.replace("MessageMedia", "").lower()


def format_reactions(reactions: dict | None) -> str:
    """Format reactions as emoji: count pairs."""
    if reactions is None:
        return ""

    results = reactions.get("results", [])
    if not results:
        return ""

    reaction_parts = []
    for r in results:
        reaction = r.get("reaction", {})
        emoticon = reaction.get("emoticon", "")
        count = r.get("count", 0)
        if emoticon:
            reaction_parts.append(f"{emoticon}: {count}")

    return ", ".join(reaction_parts)


def parse_datetime(date_str: str | None) -> tuple[str, str]:
    """Parse ISO datetime string to date and time strings."""
    if not date_str:
        return "", ""

    try:
        dt = datetime.fromisoformat(date_str)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
    except ValueError:
        return "", ""


def export_posts_to_xlsx(channel_dir: Path, output_path: Path):
    """Export posts from a channel to xlsx file."""
    posts_file = channel_dir / "posts" / "posts.json"
    channel_info_file = channel_dir / "channel_info.json"

    if not posts_file.exists():
        raise FileNotFoundError(f"Posts file not found: {posts_file}")

    with open(posts_file, "r", encoding="utf-8") as f:
        posts = json.load(f)

    # Load channel username for post links
    channel_username = ""
    if channel_info_file.exists():
        with open(channel_info_file, "r", encoding="utf-8") as f:
            channel_info = json.load(f)
            channel_username = channel_info.get("username", "")

    wb = Workbook()
    ws = wb.active
    ws.title = "Posts"

    # Header row
    headers = ["post_id", "link", "media_type", "group_id", "date", "time", "reactions", "message"]
    ws.append(headers)

    # Data rows
    for post in posts:
        post_id = post.get("id", "")
        link = f"https://t.me/{channel_username}/{post_id}" if channel_username else ""
        media_type = get_media_type(post.get("media"))
        group_id = post.get("grouped_id", "") or ""
        date_str, time_str = parse_datetime(post.get("date"))
        message = post.get("message", "") or ""
        reactions = format_reactions(post.get("reactions"))

        ws.append([post_id, link, media_type, group_id, date_str, time_str, reactions, message])

    # Adjust column widths
    ws.column_dimensions["A"].width = 12  # post_id
    ws.column_dimensions["B"].width = 35  # link
    ws.column_dimensions["C"].width = 12  # media_type
    ws.column_dimensions["D"].width = 15  # group_id
    ws.column_dimensions["D"].number_format = numbers.FORMAT_TEXT
    ws.column_dimensions["E"].width = 12  # date
    ws.column_dimensions["F"].width = 10  # time
    ws.column_dimensions["G"].width = 40  # reactions
    ws.column_dimensions["H"].width = 80  # message

    wb.save(output_path)
    print(f"Exported {len(posts)} posts to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Export channel posts to xlsx file")
    parser.add_argument("channel", help="Channel name (directory name in data/)")
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Output xlsx file path (default: data/<channel>/posts.xlsx)"
    )
    args = parser.parse_args()

    channel_dir = Path("data") / args.channel
    if not channel_dir.exists():
        print(f"Error: Channel directory not found: {channel_dir}")
        return 1

    output_path = Path(args.output) if args.output else channel_dir / f"{args.channel}.xlsx"

    export_posts_to_xlsx(channel_dir, output_path)
    return 0


if __name__ == "__main__":
    exit(main())