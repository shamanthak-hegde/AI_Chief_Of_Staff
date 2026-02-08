# Slack Data Extraction Agent

This agent extracts message history from a specified Slack channel and exports it to a JSON file. The JSON output is structured to be used for knowledge graph generation.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd AI_Chief_Of_Staff
    ```

2.  **Set up Virtual Environment and Install Dependencies:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Configure environment variables:**
    -   Copy `.env.example` to `.env`:
        ```bash
        cp .env.example .env
        ```
    -   Edit `.env` and fill in your `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID`.
        -   **SLACK_BOT_TOKEN:** Obtaining a Bot User OAuth Token from your Slack App configuration (OAuth & Permissions).
        -   **SLACK_CHANNEL_ID:** Right-click on the channel in Slack -> "Copy Link" -> The ID is the last part of the URL (e.g., `C0123456789`).

    > **Need help?** See [SLACK_SETUP.md](SLACK_SETUP.md) for a detailed step-by-step guide on creating the App and getting tokens.

## Usage

Run the script to fetch messages and export to `slack_data.json`:

```bash
# Ensure your virtual environment is activated
source venv/bin/activate
python slack_reader.py
```

## Output Format

The script generates `slack_data.json` with the following structure:

```json
[
  {
    "user": "U12345678",
    "text": "Hello world",
    "ts": "1678886400.000200",
    "reactions": [
        {"name": "thumbsup", "users": ["U87654321"], "count": 1}
    ],
    "replies": [
      {
        "user": "U87654321",
        "text": "Hi there!",
        "ts": "1678886500.000300"
      }
    ]
  }
]
```