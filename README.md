## AutoDisMediaSend

A Python CLI tool that scans a directory for media files and sends them to a Discord channel, preferring MP4+GIF pairs of the same media, and handling segmented videos and segmented pairs in nested folders. It can read the channel history and skip files that have already been uploaded (by filename), with a flag to bypass this filter.

This tool complements the media generation workflow in `disdrop` by automating the final delivery step to Discord. See `disdrop` here: `[0-FoxHunt-0/disdrop]`(https://github.com/0-FoxHunt-0/disdrop/tree/master).

### Features

- Pair detection: send MP4+GIF together when both exist
- Single-file sending when no pair is available
- Segmented media support (e.g., part1/part2 or nested segment folders)
- Dedupe against existing channel uploads by filename
- Optional flag to ignore dedupe and send all
- Windows-friendly, zero external binaries required for upload

### Requirements

- Python 3.9+
- A Discord token (recommended: Bot token)

### Install

1. Create a virtual environment and install deps

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Usage

```bash
python main.py send "D:\\path\\to\\input" "https://discord.com/channels/<guild_id>/<channel_id>" --token <DISCORD_TOKEN>
```

Options:

- `--token` or `DISCORD_TOKEN` env var: Discord token
- `--token-type` = `bot` (default) or `user`
- `--ignore-dedupe`: send all files regardless of channel history
- `--dry-run`: print what would be sent without uploading
- `--history-limit`: max messages to scan for dedupe (default: 1000)
- `--request-timeout`: seconds for history requests (default: 30)
- `--upload-timeout`: seconds for upload requests (default: 120)
- `--delay-seconds`: delay between messages (default: 1.0)
- `--max-file-mb`: size cap per file for sending (default: 8.0)
- `--skip-oversize/--no-skip-oversize`: skip files over cap (default: skip)

Examples:

```bash
# Send with dedupe (default) using bot token from env var
set DISCORD_TOKEN=xxxxxxxxxxxxxxxx
python main.py send "D:\\Media\\Output" "https://discord.com/channels/123/456"

# Force send everything
python main.py send "D:\\Media\\Output" "https://discord.com/channels/123/456" --token xxxxx --ignore-dedupe

# Dry run
python main.py send "D:\\Media\\Output" "https://discord.com/channels/123/456" --token xxxxx --dry-run
```

### How pairing and segmentation works

- Files are scanned recursively. Extensions considered: `.mp4` and `.gif`.
- Files are grouped by a normalized root name. Common segment suffixes are recognized (e.g., `_part1`, `-part02`, `_seg3`, `(1)`).
- For each root:
  - If both MP4 and GIF exist with the same segment index, they are sent together in a single message.
  - If only one exists, it is sent as a single.
  - For non-segmented items, a single pair is sent if both formats exist; otherwise the single file is sent.

### Dedupe logic

- The tool fetches recent messages from the channel and collects attachment filenames from their URLs (e.g., `https://cdn.discordapp.com/.../<filename>.mp4?...`).
- If a file's basename already appears in channel history, it is skipped, unless `--ignore-dedupe` is provided.

### Notes

- This tool complements `disdrop` for producing size-constrained media. See `disdrop` here: `[0-FoxHunt-0/disdrop]`(https://github.com/0-FoxHunt-0/disdrop/tree/master)
- Discord message attachments are limited to 10 per message; this tool only sends 1–2 at a time.

### Disclaimer

- Using a User token violates Discord's ToS. Prefer a Bot token placed in your server with send-message permissions.
