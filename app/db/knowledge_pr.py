import json
from typing import Optional

from app.db.session import get_conn


def create_knowledge_pr(
    source_turn_id: Optional[int],
    status: str,
    extracted_json: dict,
    model: Optional[str],
    title: Optional[str],
) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO knowledge_prs (source_turn_id, status, extracted_json, model, title)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    source_turn_id,
                    status,
                    json.dumps(extracted_json),
                    model,
                    title,
                ),
            )
            pr_id = cursor.fetchone()[0]
        conn.commit()
        return pr_id
    finally:
        conn.close()
