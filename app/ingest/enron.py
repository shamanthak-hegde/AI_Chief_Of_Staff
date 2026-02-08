import os
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
from typing import Dict, Iterable, List, Optional, Tuple

from app.db.session import get_conn


def _normalize_email(addr: str) -> Optional[str]:
    if not addr:
        return None
    cleaned = addr.strip().lower()
    return cleaned or None


def _parse_date(value: Optional[str]) -> datetime:
    if value:
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    return datetime.now(timezone.utc)


def _extract_recipients(headers: List[str]) -> List[str]:
    addresses = []
    for header in headers:
        addresses.extend(getaddresses([header]))
    emails = []
    for _, addr in addresses:
        normalized = _normalize_email(addr)
        if normalized:
            emails.append(normalized)
    # Dedup while preserving order
    seen = set()
    unique = []
    for email in emails:
        if email in seen:
            continue
        seen.add(email)
        unique.append(email)
    return unique


def _message_body(message) -> str:
    if message.is_multipart():
        parts = []
        for part in message.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain" and not part.get_filename():
                try:
                    parts.append(part.get_content().strip())
                except Exception:
                    payload = part.get_payload(decode=True) or b""
                    parts.append(payload.decode(errors="replace").strip())
        return "\n\n".join([p for p in parts if p])
    try:
        return message.get_content().strip()
    except Exception:
        payload = message.get_payload(decode=True) or b""
        return payload.decode(errors="replace").strip()


def _iter_email_files(root: str) -> Iterable[str]:
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            if name.startswith("."):
                continue
            yield os.path.join(dirpath, name)


def ingest_enron(path: str, limit: int = 0) -> Dict[str, int]:
    count_messages = 0
    count_people = 0
    count_recipients = 0
    seen_files = 0

    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            for file_path in _iter_email_files(path):
                if limit and count_messages >= limit:
                    break

                try:
                    with open(file_path, "rb") as handle:
                        message = BytesParser(policy=policy.default).parse(handle)
                except Exception:
                    continue

                message_id = message.get("Message-ID")
                external_id = message_id.strip() if message_id else file_path

                cursor.execute(
                    "SELECT id FROM messages WHERE platform = %s AND external_id = %s",
                    ("enron", external_id),
                )
                if cursor.fetchone():
                    continue

                sender_raw = message.get("From", "")
                sender_addresses = getaddresses([sender_raw])
                sender_email = None
                if sender_addresses:
                    _, addr = sender_addresses[0]
                    sender_email = _normalize_email(addr)

                sender_person_id = None
                if sender_email:
                    cursor.execute("SELECT id FROM people WHERE handle = %s", (sender_email,))
                    row = cursor.fetchone()
                    if row:
                        sender_person_id = row[0]
                    else:
                        cursor.execute(
                            "INSERT INTO people (handle, display_name, email) VALUES (%s, %s, %s) RETURNING id",
                            (sender_email, None, sender_email),
                        )
                        sender_person_id = cursor.fetchone()[0]
                        count_people += 1

                subject = message.get("Subject")
                timestamp = _parse_date(message.get("Date"))
                body = _message_body(message)

                cursor.execute(
                    """
                    INSERT INTO messages
                        (platform, external_id, ts, sender_person_id, subject, text, raw_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    ("enron", external_id, timestamp, sender_person_id, subject, body, None),
                )
                message_id = cursor.fetchone()[0]
                count_messages += 1

                recipient_headers = [
                    message.get("To", ""),
                    message.get("Cc", ""),
                    message.get("Bcc", ""),
                ]
                recipients = _extract_recipients(recipient_headers)
                for recipient in recipients:
                    cursor.execute("SELECT id FROM people WHERE handle = %s", (recipient,))
                    row = cursor.fetchone()
                    if row:
                        recipient_id = row[0]
                    else:
                        cursor.execute(
                            "INSERT INTO people (handle, display_name, email) VALUES (%s, %s, %s) RETURNING id",
                            (recipient, None, recipient),
                        )
                        recipient_id = cursor.fetchone()[0]
                        count_people += 1

                    cursor.execute(
                        """
                        INSERT INTO message_recipients (message_id, recipient_person_id, kind)
                        VALUES (%s, %s, %s)
                        """,
                        (message_id, recipient_id, "to"),
                    )
                    count_recipients += 1

                seen_files += 1

        conn.commit()
    finally:
        conn.close()

    return {
        "messages": count_messages,
        "people": count_people,
        "recipients": count_recipients,
        "files_processed": seen_files,
    }
