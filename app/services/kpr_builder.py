import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.db.session import get_conn
from app.llm.openai_client import OpenAIClient
from app.schemas.extraction import Extraction
from app.services.embeddings import cosine_similarity
from app.services.extractor import ExtractorService


SIMILARITY_THRESHOLD = 0.78


@dataclass
class PRResult:
    pr_id: int
    changes: int


class KPRBuilder:
    def __init__(
        self,
        extractor: Optional[ExtractorService] = None,
        client: Optional[OpenAIClient] = None,
    ) -> None:
        self._extractor = extractor or ExtractorService(client=client)
        self._client = client or OpenAIClient()

    def build_from_turn(self, turn_id: int) -> PRResult:
        conn = get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT text FROM turns WHERE id = %s", (turn_id,))
                row = cursor.fetchone()
                if not row:
                    raise ValueError("Turn not found")
                turn_text = row[0]

                extraction_result = self._extractor.extract_turn(turn_text)
                extraction = extraction_result.extraction

                cursor.execute(
                    """
                    INSERT INTO knowledge_prs (source_turn_id, status, extracted_json, model, title)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        turn_id,
                        "needs_review",
                        json.dumps(extraction.model_dump()),
                        self._client.model_name,
                        f"Turn {turn_id} updates",
                    ),
                )
                pr_id = cursor.fetchone()[0]

                change_count = 0
                items = self._build_items(extraction)
                for item in items:
                    proposed_summary = item["summary"]
                    item_type = item["type"]
                    title = item["title"]

                    embedding = self._client.embed(proposed_summary)
                    match = self._find_best_match(cursor, embedding)

                    if match:
                        truth_item_id, current_version_id, similarity = match
                        diff_summary = "Proposed update to existing truth item"
                    else:
                        truth_item_id = self._create_truth_item(cursor, item_type, title)
                        self._store_embedding(cursor, truth_item_id, embedding)
                        current_version_id = None
                        similarity = 0.0
                        diff_summary = "New truth item"

                    cursor.execute(
                        """
                        INSERT INTO pr_changes
                            (pr_id, truth_item_id, previous_version_id, proposed_summary, diff_summary, similarity)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            pr_id,
                            truth_item_id,
                            current_version_id,
                            proposed_summary,
                            diff_summary,
                            similarity,
                        ),
                    )
                    change_count += 1

            conn.commit()
            return PRResult(pr_id=pr_id, changes=change_count)
        finally:
            conn.close()

    def _build_items(self, extraction: Extraction) -> List[Dict[str, str]]:
        items: List[Dict[str, str]] = []
        for decision in extraction.decisions:
            summary = decision.details or decision.title
            items.append(
                {
                    "type": "decision",
                    "title": decision.title,
                    "summary": summary,
                }
            )
        for claim in extraction.claims:
            items.append(
                {
                    "type": "claim",
                    "title": claim.statement[:120],
                    "summary": claim.statement,
                }
            )
        return items

    def _find_best_match(
        self, cursor, embedding: List[float]
    ) -> Optional[Tuple[int, Optional[int], float]]:
        cursor.execute(
            """
            SELECT ti.id, ti.current_version_id, tie.embedding
            FROM truth_items ti
            JOIN truth_item_embeddings tie ON tie.truth_item_id = ti.id
            """
        )
        best: Optional[Tuple[int, Optional[int], float]] = None
        for truth_item_id, current_version_id, emb_json in cursor.fetchall():
            stored = emb_json
            if isinstance(stored, str):
                try:
                    stored = json.loads(stored)
                except Exception:
                    continue
            similarity = cosine_similarity(embedding, stored)
            if best is None or similarity > best[2]:
                best = (truth_item_id, current_version_id, similarity)
        if best and best[2] >= SIMILARITY_THRESHOLD:
            return best
        return None

    def _create_truth_item(self, cursor, item_type: str, title: str) -> int:
        cursor.execute(
            """
            INSERT INTO truth_items (type, title)
            VALUES (%s, %s)
            RETURNING id
            """,
            (item_type, title),
        )
        return cursor.fetchone()[0]

    def _store_embedding(self, cursor, truth_item_id: int, embedding: List[float]) -> None:
        cursor.execute(
            """
            INSERT INTO truth_item_embeddings (truth_item_id, embedding)
            VALUES (%s, %s)
            """,
            (truth_item_id, json.dumps(embedding)),
        )
