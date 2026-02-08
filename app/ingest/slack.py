import json
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from app.db.session import get_conn


def _parse_ts(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _iter_messages(raw: List[Dict]) -> Iterable[Dict]:
    for msg in raw:
        yield msg
        for reply in msg.get("replies", []) or []:
            yield reply


def ingest_slack(path: str, limit: int = 0) -> Dict[str, int]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    count_messages = 0
    count_people = 0

    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            for msg in _iter_messages(data):
                if limit and count_messages >= limit:
                    break

                external_id = msg.get("external_id")
                if not external_id:
                    channel_id = msg.get("channel_id")
                    ts = msg.get("ts")
                    if channel_id and ts:
                        external_id = f"{channel_id}:{ts}"

                if not external_id:
                    continue

                cursor.execute(
                    "SELECT id FROM messages WHERE platform = %s AND external_id = %s",
                    ("slack", external_id),
                )
                if cursor.fetchone():
                    continue

                user_id = msg.get("user")
                sender_person_id = None
                if user_id:
                    cursor.execute("SELECT id FROM people WHERE handle = %s", (user_id,))
                    row = cursor.fetchone()
                    if row:
                        sender_person_id = row[0]
                    else:
                        cursor.execute(
                            "INSERT INTO people (handle, display_name, email) VALUES (%s, %s, %s) RETURNING id",
                            (user_id, None, None),
                        )
                        sender_person_id = cursor.fetchone()[0]
                        count_people += 1

                cursor.execute(
                    """
                    INSERT INTO messages
                        (platform, external_id, ts, sender_person_id, channel_id, thread_id, text, raw_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        "slack",
                        external_id,
                        _parse_ts(msg.get("ts")),
                        sender_person_id,
                        msg.get("channel_id"),
                        msg.get("thread_id"),
                        msg.get("text"),
                        json.dumps(msg),
                    ),
                )
                count_messages += 1

        conn.commit()
    finally:
        conn.close()

    return {"messages": count_messages, "people": count_people}
