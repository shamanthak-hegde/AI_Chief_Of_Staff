import os
import json
import logging
from typing import List, Dict, Optional
from datetime import datetime
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SlackReader:
    def __init__(self, token: str):
        """
        Initialize the SlackReader with a bot token.
        """
        self.client = WebClient(token=token)

    def fetch_joined_channels(self) -> List[Dict]:
        """
        Fetch all channels (public, private, DM, MPDM) the bot is a member of.
        """
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
                    logging.error(f"Error fetching conversations: {response['error']}")
                    break

                channels.extend(response["channels"])

                if not response.get("has_more"):
                    break
                
                cursor = response["response_metadata"]["next_cursor"]
                logging.info(f"Discovered {len(channels)} channels so far...")

        except SlackApiError as e:
            logging.error(f"Slack API Error fetching channels: {e.response['error']}")
        
        return channels

    def fetch_history(self, channel_id: str, limit: int = 100) -> List[Dict]:
        """
        Fetch message history from a specific Slack channel.
        """
        messages = []
        try:
            cursor = None
            while True:
                response = self.client.conversations_history(
                    channel=channel_id,
                    limit=limit,
                    cursor=cursor
                )
                
                if not response["ok"]:
                    logging.error(f"Error fetching history for {channel_id}: {response['error']}")
                    break

                current_messages = response["messages"]
                messages.extend(current_messages)

                if not response.get("has_more"):
                    break
                
                cursor = response["response_metadata"]["next_cursor"]
                # logging.info(f"Fetched {len(messages)} messages from {channel_id} so far...")

        except SlackApiError as e:
            logging.error(f"Slack API Error: {e.response['error']}")
        
        return messages

    def fetch_replies(self, channel_id: str, thread_ts: str) -> List[Dict]:
        """
        Fetch threaded replies for a specific message.
        """
        replies = []
        try:
            cursor = None
            while True:
                response = self.client.conversations_replies(
                    channel=channel_id,
                    ts=thread_ts,
                    cursor=cursor
                )

                if not response["ok"]:
                    logging.error(f"Error fetching replies: {response['error']}")
                    break

                current_replies = response["messages"]
                # Filter out the parent message which is usually the first one in replies
                if current_replies and current_replies[0]['ts'] == thread_ts:
                    current_replies.pop(0)

                replies.extend(current_replies)

                if not response.get("has_more"):
                    break
                
                cursor = response["response_metadata"]["next_cursor"]

        except SlackApiError as e:
            logging.error(f"Slack API Error fetching replies: {e.response['error']}")
        
        return replies

    def process_messages(self, channel_id: str, raw_messages: List[Dict]) -> List[Dict]:
        """
        Process raw Slack messages into a clean format.
        """
        processed_data = []
        
        for msg in raw_messages:
            # Skip subtype messages (like 'channel_join', etc.) if needed, 
            # but for now we keep everything that constitutes a message.
            
            processed_msg = {
                "channel_id": channel_id,
                "user": msg.get("user"),
                "text": msg.get("text"),
                "ts": msg.get("ts"),
                "type": msg.get("type"),
                "subtype": msg.get("subtype"),
                "reactions": [],
                "replies": []
            }

            # Process reactions
            if "reactions" in msg:
                for reaction in msg["reactions"]:
                    processed_msg["reactions"].append({
                        "name": reaction["name"],
                        "users": reaction["users"],
                        "count": reaction["count"]
                    })

            # Process threaded replies
            if "thread_ts" in msg and msg.get("reply_count", 0) > 0:
                logging.debug(f"Fetching replies for thread {msg['ts']} in {channel_id}...")
                replies = self.fetch_replies(channel_id, msg["thread_ts"])
                for reply in replies:
                     processed_reply = {
                        "user": reply.get("user"),
                        "text": reply.get("text"),
                        "ts": reply.get("ts"),
                        "reactions": []
                    }
                     if "reactions" in reply:
                        for reaction in reply["reactions"]:
                            processed_reply["reactions"].append({
                                "name": reaction["name"],
                                "users": reaction["users"],
                                "count": reaction["count"]
                            })
                     processed_msg["replies"].append(processed_reply)

            processed_data.append(processed_msg)
        
        # Sort by timestamp (oldest first)
        processed_data.sort(key=lambda x: float(x['ts']))
        return processed_data

    def save_to_json(self, data: List[Dict], filename: str = "slack_data.json"):
        """
        Save processed data to a JSON file.
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logging.info(f"Successfully saved data to {filename}")
        except IOError as e:
            logging.error(f"Error saving to JSON: {e}")

def main():
    # Load environment variables
    load_dotenv()
    
    token = os.getenv("SLACK_BOT_TOKEN")
    
    if not token:
        logging.error("Missing SLACK_BOT_TOKEN in environment variables.")
        return

    reader = SlackReader(token)
    
    logging.info("Scanning for channels bot is a member of...")
    channels = reader.fetch_joined_channels()
    logging.info(f"Found {len(channels)} channels.")

    all_messages = []

    for idx, channel in enumerate(channels):
        channel_id = channel["id"]
        channel_name = channel.get("name_normalized") or channel.get("name") or "DM/Private"
        logging.info(f"Processing channel {idx+1}/{len(channels)}: {channel_name} ({channel_id})")
        
        raw_history = reader.fetch_history(channel_id, limit=200) 
        if raw_history:
            clean_data = reader.process_messages(channel_id, raw_history)
            all_messages.extend(clean_data)
    
    logging.info(f"Total messages collected: {len(all_messages)}")
    reader.save_to_json(all_messages)
    logging.info("Done.")

if __name__ == "__main__":
    main()
