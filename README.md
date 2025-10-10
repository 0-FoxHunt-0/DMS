## AutoDisMediaSend

A Python CLI tool that scans a directory for media files and sends them to a Discord channel, preferring MP4+GIF pairs of the same media, and handling segmented videos and segmented pairs in nested folders. It can read the channel history and skip files that have already been uploaded (by filename), with a flag to bypass this filter.

This tool complements the media generation workflow in `disdrop` by automating the final delivery step to Discord. See disdrop here: [0-FoxHunt-0/disdrop](https://github.com/0-FoxHunt-0/disdrop/tree/master).

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

### Install (from source)

1. Create a virtual environment and install deps

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Install (pip)

When packaged, you can install via:

```bash
pip install autodismediasend
```

This provides a console entry point `discord-send`.

### Usage

```bash
python main.py send "D:\\path\\to\\input" "https://discord.com/channels/<guild_id>/<channel_id>" --token <DISCORD_TOKEN>
```

Or via the installed command after `pip install`:

```bash
discord-send "D:\\path\\to\\input" "https://discord.com/channels/<guild_id>/<channel_id>" --token <DISCORD_TOKEN>
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
- `--max-file-mb`: size cap per file for sending (default: 10.0)
- `--skip-oversize/--no-skip-oversize`: skip files over cap (default: skip)
- `--split-by-subfolders`: when posting to Forum/Media without a thread id, split uploads into one thread for root files (if any) and one per top-level subfolder; each group checks for an existing thread by name before prompting/creating

#### Environment variables and .env

- You can set `DISCORD_TOKEN` in your environment or place it in a `.env` file in the project root. The CLI now auto-loads `.env`.
- If no token is found, you'll be prompted once at runtime, with an option to save it to `.env` for future runs.

Examples:

````bash
# Send with dedupe (default) using bot token from .env or env var
python main.py send "D:\\Media\\Output" "https://discord.com/channels/123/456"

# Force send everything
python main.py send "D:\\Media\\Output" "https://discord.com/channels/123/456" --token xxxxx --ignore-dedupe

# Dry run (reads token from .env if saved)
python main.py send "D:\\Media\\Output" "https://discord.com/channels/123/456" --dry-run

#### Relay media from one channel to another

You can relay previously uploaded media from a source channel to a destination channel. The tool downloads media to a temporary directory and re-uploads it in chronological order.

```bash
# Relay from source to destination
python main.py send \
  . \
  "https://discord.com/channels/<guild_id>/<dest_channel_id>" \
  --relay-from "https://discord.com/channels/<guild_id>/<source_channel_id>" \
  --history-limit 1000 \
  --relay-download-dir .adms_cache
````

Notes:

- The first positional argument `input_dir` is unused in relay mode; pass `.`
- Relay respects `--max-file-mb` and `--skip-oversize` for downloads/uploads
- `--history-limit` controls how many messages to scan in the source channel

```

### How pairing, segmentation, and auto-splitting work

- Files are scanned recursively. Extensions considered: `.mp4` and `.gif`.
- Files are grouped by a normalized root name. Common segment suffixes are recognized (e.g., `_part1`, `-part02`, `_seg3`, `(1)`).
- For each root:
  - If both MP4 and GIF exist with the same segment index, they are sent together in a single message.
  - If only one exists, it is sent as a single.
  - For non-segmented items, a single pair is sent if both formats exist; otherwise the single file is sent.
- GUI Auto mode: if the destination is a Forum/Media channel and "Send as single thread" is unchecked, the app will create one job for root-level files (if present) and one per top-level subfolder. It checks for existing threads by name; if none is found, it suggests a title derived from the folder name, stripping `_segments` or inferring the common segmented base when applicable.

### Dedupe logic

- The tool fetches recent messages from the channel and collects attachment filenames from their URLs (e.g., `https://cdn.discordapp.com/.../<filename>.mp4?...`).
- If a file's basename already appears in channel history, it is skipped, unless `--ignore-dedupe` is provided.

### Notes

- This tool complements `disdrop` for producing size-constrained media. See `disdrop` here: `[0-FoxHunt-0/disdrop]`(https://github.com/0-FoxHunt-0/disdrop/tree/master)
- Discord message attachments are limited to 10 per message; this tool only sends 1–2 at a time.

### Disclaimer

- Using a User token violates Discord's ToS. Prefer a Bot token placed in your server with send-message permissions.
```
