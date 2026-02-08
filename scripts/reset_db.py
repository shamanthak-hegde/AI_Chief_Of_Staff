from app.db.session import get_conn

TABLES = [
    "pr_conflicts",
    "pr_stakeholders",
    "pr_changes",
    "knowledge_prs",
    "truth_item_embeddings",
    "truth_versions",
    "truth_items",
    "topics",
    "turn_messages",
    "turns",
    "message_recipients",
    "messages",
    "people",
    "comm_edges",
    "embedding_cache",
    "extraction_cache",
]


def reset_db() -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            for table in TABLES:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    reset_db()
