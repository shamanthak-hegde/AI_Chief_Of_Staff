import argparse
import json
from datetime import datetime, timezone

from app.db.init_db import init_db
from app.db.session import get_conn
from app.ingest.enron import ingest_enron
from app.ingest.slack import ingest_slack


def rebuild_turns(limit: int | None = None) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM turn_messages")
            cursor.execute("DELETE FROM turns")

            sql = """
                SELECT id, platform, channel_id, thread_id, sender_person_id, ts, text
                FROM messages
                ORDER BY ts
            """
            if limit:
                sql += " LIMIT %s"
                cursor.execute(sql, (limit,))
            else:
                cursor.execute(sql)

            count = 0
            for msg_id, platform, channel_id, thread_id, sender_person_id, ts, text in cursor.fetchall():
                cursor.execute(
                    """
                    INSERT INTO turns
                        (platform, channel_id, thread_id, sender_person_id, start_ts, end_ts, text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        platform,
                        channel_id,
                        thread_id,
                        sender_person_id,
                        ts,
                        ts,
                        text or "",
                    ),
                )
                turn_id = cursor.fetchone()[0]
                cursor.execute(
                    "INSERT INTO turn_messages (turn_id, message_id) VALUES (%s, %s)",
                    (turn_id, msg_id),
                )
                count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def rebuild_comm_edges() -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM comm_edges")
            cursor.execute(
                """
                SELECT m.sender_person_id, mr.recipient_person_id, COUNT(*), MAX(m.ts)
                FROM messages m
                JOIN message_recipients mr ON mr.message_id = m.id
                WHERE m.sender_person_id IS NOT NULL
                GROUP BY m.sender_person_id, mr.recipient_person_id
                """
            )
            rows = cursor.fetchall()
            for src, dst, weight, last_ts in rows:
                cursor.execute(
                    """
                    INSERT INTO comm_edges (src_person_id, dst_person_id, weight, last_ts)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (src, dst, float(weight), last_ts),
                )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def seed_prs() -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM turns ORDER BY id LIMIT 3")
            turn_ids = [row[0] for row in cursor.fetchall()]
            created = 0
            for turn_id in turn_ids:
                payload = {
                    "participants": [],
                    "topics": ["demo"],
                    "decisions": [],
                    "action_items": [],
                    "claims": [],
                }
                cursor.execute(
                    """
                    INSERT INTO knowledge_prs (source_turn_id, status, extracted_json, model, title)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (turn_id, "needs_review", json.dumps(payload), "demo", f"Demo PR {turn_id}"),
                )
                created += 1
        conn.commit()
        return created
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--enron-path", default="data/enron_mail_data")
    parser.add_argument("--enron-limit", type=int, default=200)
    parser.add_argument("--slack-path", default="")
    parser.add_argument("--slack-limit", type=int, default=200)
    parser.add_argument("--with-prs", action="store_true")
    args = parser.parse_args()

    init_db()
    ingest_enron(args.enron_path, args.enron_limit)
    if args.slack_path:
        ingest_slack(args.slack_path, args.slack_limit)
    rebuild_turns()
    rebuild_comm_edges()

    if args.with_prs:
        seed_prs()


if __name__ == "__main__":
    main()
