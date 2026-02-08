import hashlib
import json
from typing import Optional

from app.db.session import get_conn


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def get_embedding_cache(model: str, text: str) -> Optional[list[float]]:
    digest = text_hash(text)
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT embedding FROM embedding_cache WHERE model = %s AND text_hash = %s",
                (model, digest),
            )
            row = cursor.fetchone()
            if not row:
                return None
            embedding = row[0]
            if isinstance(embedding, str):
                try:
                    return json.loads(embedding)
                except Exception:
                    return None
            return embedding
    finally:
        conn.close()


def set_embedding_cache(model: str, text: str, embedding: list[float]) -> None:
    digest = text_hash(text)
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO embedding_cache (model, text_hash, embedding)
                VALUES (%s, %s, %s)
                ON CONFLICT (model, text_hash) DO UPDATE SET embedding = EXCLUDED.embedding
                """,
                (model, digest, json.dumps(embedding)),
            )
        conn.commit()
    finally:
        conn.close()


def get_extraction_cache(model: str, turn_id: int, text: str) -> Optional[dict]:
    digest = text_hash(text)
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT extracted_json, text_hash
                FROM extraction_cache
                WHERE model = %s AND turn_id = %s
                """,
                (model, turn_id),
            )
            row = cursor.fetchone()
            if not row:
                return None
            extracted_json, cached_hash = row
            if cached_hash != digest:
                return None
            if isinstance(extracted_json, str):
                try:
                    return json.loads(extracted_json)
                except Exception:
                    return None
            return extracted_json
    finally:
        conn.close()


def set_extraction_cache(model: str, turn_id: int, text: str, extracted_json: dict) -> None:
    digest = text_hash(text)
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO extraction_cache (model, turn_id, text_hash, extracted_json)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (model, turn_id)
                DO UPDATE SET extracted_json = EXCLUDED.extracted_json, text_hash = EXCLUDED.text_hash
                """,
                (model, turn_id, digest, json.dumps(extracted_json)),
            )
        conn.commit()
    finally:
        conn.close()
