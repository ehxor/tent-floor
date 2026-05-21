# Tent Floor

Live transcription and monitoring of emergency radio scanner streams with two-tone pager detection, CAD incident polling, and web-based feed display.

## Features

- **Live Transcription**: Real-time speech-to-text using [whisper.cpp](https://github.com/ggerganov/whisper.cpp) with GPU acceleration (Vulkan/Metal)
- **Two-Tone Pager Detection**: Identifies Motorola Quick Call II / Plectron paging tones and maps them to specific units
- **PulsePoint Integration**: Polls PulsePoint CAD data for incident tracking and unit status updates
- **Nanaimo Fire API**: Monitors Nanaimo Fire Rescue's public incident feed
- **BC Wildfire Tracking**: Polls BC Wildfire Service for wildfire incidents within a configurable geographic polygon, tracking status, size, and fire-of-note changes
- **Multi-Stream Support**: Process multiple scanner streams simultaneously with group-based configuration
- **Web Feed**: Cloudflare Workers-based live feed with auto-refresh UI
- **Discord Integration**: Optional Discord webhook notifications

## Requirements

- Python 3.8+
- [whisper.cpp](https://github.com/ggerganov/whisper.cpp) built with GPU support (`-DGGML_VULKAN=ON` or `-DGGML_METAL=ON`)
- FFmpeg (for stream decoding)
- Cloudflare account (optional, for creating a web feed)

## Installation

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

2. Clone and build whisper.cpp:

```bash
git clone https://github.com/ggerganov/whisper.cpp
cd whisper.cpp

# Linux (Vulkan)
cmake -B build -DGGML_VULKAN=ON
cmake --build build -j$(nproc)

# macOS (Metal)
cmake -B build -DGGML_METAL=ON
cmake --build build -j$(sysctl -n hw.ncpu)

# Download model
./models/download-ggml-model.sh large-v3
```

3. (Optional) Deploy the web feed:

```bash
cd web/scanner-feed
npx wrangler deploy
npx wrangler secret put INGEST_TOKEN  # Set your auth token
```

## Configuration

Create a `config.json` (see `config.json` in this repo for an example):

```json
{
  "whisper": {
    "model": "large-v3"
  },
  "groups": {
    "vancouver-island": {
      "outputs": {
        "feed_url": "https://your-feed.workers.dev",
        "feed_token": "your-ingest-token",
        "discord_webhook": "https://discordapp.com/api/webhooks/..."
      },
      "streams": [
        {
          "name": "Mid Island",
          "url": "https://icecast0.scanbc.com/nanaimo",
          "jargon": "jargon.txt",
          "tone_lookup": "tone_lookup_vancouver_island.json"
        }
      ],
      "pollers": [
        {
          "type": "pulsepoint",
          "agency": "EMS1201",
          "unit_prefix": ["1"]
        },
        {
          "type": "bc_wildfire",
          "polygon": [[-121.5, 51.5], [-119.0, 51.5], [-119.0, 49.5], [-121.5, 49.5]],
          "fire_centre_code": "50"
        }
      ]
    }
  }
}
```

### Configuration Fields

| Field | Description |
|-------|-------------|
| `whisper.bin` | Path to whisper-cli binary (auto-detected if omitted) |
| `whisper.model` | Model name (default: `large-v3`) |
| `groups` | Named groups of streams + outputs |
| `outputs.feed_url` | Cloudflare Worker URL for web feed |
| `outputs.feed_token` | Bearer token for feed ingestion |
| `outputs.discord_webhook` | Optional Discord webhook URL |
| `streams[].jargon` | File with local terms for transcription context |
| `streams[].tone_lookup` | Path to tone lookup JSON for this stream (optional) |
| `pollers[].type` | `pulsepoint`, `nanaimo_fire`, or `bc_wildfire` |

### BC Wildfire Poller

Polls the BC Wildfire Service API every 10 minutes for active wildfires, filtered to a geographic polygon. Emits events when a fire's status, estimated size, fire-of-note designation, or name changes.

| Field | Required | Description |
|-------|----------|-------------|
| `polygon` | No | Array of `[lon, lat]` pairs (GeoJSON order) defining the area of interest (min 3 points). Compatible with exports from tools like geojson.io. If omitted, all incidents returned for the given fire centre are tracked |
| `fire_centre_code` | No | Fire centre code to pre-filter the API query (e.g. `50` for Kamloops) |

If `polygon` is set, only fires whose coordinates fall inside it are tracked. Use `--dump` in standalone mode to test your polygon against live data.

## Usage

Run with config file (recommended):

```bash
python scanner_transcribe_gpu.py --config config.json
```

Quick single-stream mode:

```bash
python scanner_transcribe_gpu.py https://stream-url.example.com/stream \
  --jargon jargon.txt \
  --whisper-bin ./whisper.cpp/build/bin/whisper-cli
```

## Tuning

### Jargon File (`jargon.txt`)

Provides context terms to improve transcription accuracy for local place names, unit identifiers, and dispatch terminology:

```
# Place names
Nanaimo
Parksville
Qualicum

# Units
Nanaimo Engine 3
Nanaimo Rescue 1

# Dispatch terms
Code 3
Structure fire
BCEHS
```

### Tone Lookup (per-stream)

Tone lookup is configured per-stream via the `tone_lookup` field in each stream entry. Each stream points to its own lookup JSON file mapping two-tone frequency pairs to unit names. This allows different regions to have separate, non-overlapping tone mappings.

```json
{
  "634.1/600.9": "Honeymoon Bay Fire Department",
  "1210.0/1277.5": "120-Alpha-3"
}
```

Each stream gets its own `TwoToneDetector` instance, so tone state is fully isolated between streams. Unknown tones are logged per-stream to `unknown_tones_{stream_name}.log` (e.g., `unknown_tones_mid_island.log`). Lookup files are reloaded every 5 minutes.

Streams without a `tone_lookup` entry skip tone detection entirely. Use `--no-tones` to disable tone detection for all streams.

Listen to dispatches after unknown tones and add mappings to the appropriate stream's lookup file to identify future pages.

### VAD Parameters

In `scanner_transcribe_gpu.py`, adjust for your audio:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SPECTRAL_FLUX_THRESHOLD` | 0.5 | Speech detection sensitivity |
| `SILENCE_TO_SPLIT_MS` | 600 | Silence duration to split transmissions |
| `MIN_TRANSMISSION_S` | 1.0 | Minimum transmission length to transcribe |

## Standalone Pollers

Run PulsePoint poller alone:

```bash
python pulsepoint_poller.py --agency EMS1201 --unit-prefix 1
```

Run Nanaimo Fire poller alone:

```bash
python nanaimo_fire_poller.py
```

Run BC Wildfire poller alone (polygon defines the area of interest):

```bash
python bc_wildfire_poller.py --polygon "-121.5,51.5 -119.0,51.5 -119.0,49.5 -121.5,49.5" --dump
python bc_wildfire_poller.py --polygon "-121.5,51.5 -119.0,51.5 -119.0,49.5 -121.5,49.5" --fire-centre 50
```

## Sample Streams

BC scanner streams (ScanBC, Broadcastify):

| Region | URL |
|--------|-----|
| Nanaimo/Mid Island | `https://icecast0.scanbc.com/nanaimo` |
| Kamloops | `https://icecast0.scanbc.com/kamloops` |
| Cowichan Valley | `https://broadcastify.cdnstream1.com/44935` |
| Comox Valley | `https://broadcastify.cdnstream1.com/31760` |
| North Island | `https://broadcastify.cdnstream1.com/21518` |

## License

MIT
