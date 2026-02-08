import os
import json
import logging
import time
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("slack_monitor.log"),
        logging.StreamHandler()
    ]
)

class SlackMonitor:
    def __init__(self, token: str, data_file: str = "slack_data.json", state_file: str = "monitor_state.json"):
        self.client = WebClient(token=token)
        self.data_file = data_file
        self.state_file = state_file
        self.channel_states = {}  # {channel_id: last_ts}
        
        self._load_state()

    def _load_state(self):
        """Load the last known timestamp for each channel from state file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    self.channel_states = json.load(f)
                    logging.info(f"Loaded state for {len(self.channel_states)} channels.")
            except json.JSONDecodeError:
                logging.warning("Could not read state file. Starting fresh.")
                self.channel_states = {}
        else:
             logging.info("No state file found. Will initialize baseline.")

    def _save_state(self):
        """Save the current channel states to file."""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.channel_states, f, indent=2)
        except IOError as e:
            logging.error(f"Error saving state: {e}")

    def fetch_joined_channels(self) -> List[Dict]:
        """Fetch all channels the bot is a member of."""
        channels = []
        try:
            cursor = None
            while True:
                response = self.client.users_conversations(
                    types="public_channel,private_channel,im,mpim",
                    cursor=cursor,
                    limit=200
                )
                if not response["ok"]:
                    break
                channels.extend(response["channels"])
                if not response.get("has_more"):
                    break
                cursor = response["response_metadata"]["next_cursor"]
        except SlackApiError as e:
            logging.error(f"Error fetching channels: {e}")
        return channels

    def _initialize_baseline_for_channel(self, channel_id: str):
        """Fetch the latest message to set the starting point if no state exists."""
        try:
            response = self.client.conversations_history(
                channel=channel_id,
                limit=1
            )
            if response["ok"] and response["messages"]:
                last_ts = float(response["messages"][0]["ts"])
                self.channel_states[channel_id] = last_ts
                logging.info(f"Initialized baseline for {channel_id}: {last_ts}")
            else:
                self.channel_states[channel_id] = time.time()
                logging.info(f"Initialized baseline for {channel_id} with current time")
        except SlackApiError as e:
            logging.error(f"Error initializing baseline for {channel_id}: {e}")

    def fetch_new_messages(self, channel_id: str, last_known_ts: float) -> List[Dict]:
        """Fetch messages newer than last_known_ts for a specific channel."""
        messages = []
        try:
            response = self.client.conversations_history(
                channel=channel_id,
                oldest=str(last_known_ts),
                inclusive=False 
            )
            
            if response["ok"]:
                new_msgs = response["messages"]
                if new_msgs:
                    # Sort by timestamp ascending
                    new_msgs.sort(key=lambda x: float(x['ts']))
                    messages = new_msgs
        except SlackApiError as e:
            logging.error(f"Error checking new messages in {channel_id}: {e}")
            
        return messages

    def fetch_replies(self, channel_id: str, thread_ts: str) -> List[Dict]:
        """Fetch replies for a specific thread."""
        replies = []
        try:
            response = self.client.conversations_replies(
                channel=channel_id,
                ts=thread_ts
            )
            if response["ok"]:
                msgs = response["messages"]
                if msgs and msgs[0]['ts'] == thread_ts:
                    msgs.pop(0)
                replies = msgs
        except SlackApiError as e:
            logging.error(f"Error fetching replies: {e}")
        return replies

    def process_and_save(self, channel_id: str, new_messages: List[Dict]):
        """Process new messages and append to JSON."""
        if not new_messages:
            return

        current_data = []
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, 'r', encoding='utf-8') as f:
                    current_data = json.load(f)
            except json.JSONDecodeError:
                current_data = []

        for msg in new_messages:
            processed_msg = {
                "channel_id": channel_id,
                "user": msg.get("user"),
                "text": msg.get("text"),
                "ts": msg.get("ts"),
                "type": msg.get("type"),
                "timestamp_human": datetime.fromtimestamp(float(msg['ts'])).strftime('%Y-%m-%d %H:%M:%S'),
                "reactions": [],
                "replies": []
            }

            if "reactions" in msg:
                for reaction in msg["reactions"]:
                    processed_msg["reactions"].append({
                        "name": reaction["name"],
                        "users": reaction["users"],
                        "count": reaction["count"]
                    })

            if "thread_ts" in msg and msg.get("reply_count", 0) > 0:
                logging.debug(f"Fetching replies for thread {msg['ts']}...")
                replies = self.fetch_replies(channel_id, msg["thread_ts"])
                for reply in replies:
                     processed_reply = {
                        "user": reply.get("user"),
                        "text": reply.get("text"),
                        "ts": reply.get("ts"),
                        "timestamp_human": datetime.fromtimestamp(float(reply['ts'])).strftime('%Y-%m-%d %H:%M:%S')
                    }
                     processed_msg["replies"].append(processed_reply)

            logging.info(f"New Message in {channel_id}: {processed_msg['text'][:50]}...")
            current_data.append(processed_msg)

        try:
            with open(self.data_file, 'w', encoding='utf-8') as f:
                json.dump(current_data, f, indent=2, ensure_ascii=False)
            logging.info(f"Saved {len(new_messages)} new message(s) from {channel_id}")
        except IOError as e:
            logging.error(f"Error writing to file: {e}")

    def run(self, interval_seconds: int = 10):
        """Start the monitor loop."""
        logging.info(f"Starting Slack Monitor (All Channels). Interval: {interval_seconds}s")
        
        try:
            while True:
                # 1. Discover channels
                channels = self.fetch_joined_channels()
                logging.info(f"Scanning {len(channels)} joined channels...")

                for channel in channels:
                    channel_id = channel["id"]
                    
                    # 2. Check state
                    if channel_id not in self.channel_states:
                        self._initialize_baseline_for_channel(channel_id)
                        # Skip fetching immediately after baseline init to avoid old spam
                        # or fetch only if you want "messages since startup"
                        continue 
                    
                    last_known_ts = self.channel_states[channel_id]

                    # 3. Fetch new messages
                    new_msgs = self.fetch_new_messages(channel_id, last_known_ts)
                    
                    if new_msgs:
                        self.process_and_save(channel_id, new_msgs)
                        # 4. Update state
                        self.channel_states[channel_id] = float(new_msgs[-1]['ts'])
                
                # 5. Save state
                self._save_state()
                
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logging.info("Monitor stopped by user.")

def main():
    load_dotenv()
    token = os.getenv("SLACK_BOT_TOKEN")

    if not token:
        logging.error("Missing SLACK_BOT_TOKEN in .env")
        return

    monitor = SlackMonitor(token)
    monitor.run(interval_seconds=10)

if __name__ == "__main__":
    main()
