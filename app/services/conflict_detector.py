import time
from dataclasses import dataclass
from typing import Optional

from app.db.session import get_conn
from app.llm.openai_client import OpenAIClient


RETRY_ATTEMPTS = 2
BACKOFF_BASE_SECONDS = 0.5


@dataclass
class ConflictResult:
    pr_id: int
    conflicts: int
    status: str


class ConflictDetector:
    def __init__(self, client: Optional[OpenAIClient] = None) -> None:
        self._client = client or OpenAIClient()

    def run_conflicts(self, pr_id: int) -> ConflictResult:
        conn = get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM knowledge_prs WHERE id = %s", (pr_id,))
                if not cursor.fetchone():
                    raise ValueError("PR not found")

                cursor.execute("DELETE FROM pr_conflicts WHERE pr_id = %s", (pr_id,))

                cursor.execute(
                    """
                    SELECT truth_item_id, proposed_summary, previous_version_id
                    FROM pr_changes
                    WHERE pr_id = %s
                    ORDER BY id
                    """,
                    (pr_id,),
                )
                changes = cursor.fetchall()

                conflict_count = 0
                for truth_item_id, proposed_summary, previous_version_id in changes:
                    if not previous_version_id:
                        continue

                    cursor.execute(
                        "SELECT summary FROM truth_versions WHERE id = %s",
                        (previous_version_id,),
                    )
                    row = cursor.fetchone()
                    if not row:
                        continue

                    existing_summary = row[0]
                    check = self._run_with_retries(existing_summary, proposed_summary)
                    if not check.conflict:
                        continue

                    cursor.execute(
                        """
                        INSERT INTO pr_conflicts
                            (pr_id, truth_item_id, conflict_type, existing_claim, new_claim, resolution_hint)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            pr_id,
                            truth_item_id,
                            check.conflict_type,
                            check.existing_span,
                            check.new_span,
                            check.resolution_hint,
                        ),
                    )
                    conflict_count += 1

                status = "merge_conflict" if conflict_count > 0 else "needs_review"
                cursor.execute(
                    "UPDATE knowledge_prs SET status = %s WHERE id = %s",
                    (status, pr_id),
                )

            conn.commit()
            return ConflictResult(pr_id=pr_id, conflicts=conflict_count, status=status)
        finally:
            conn.close()

    def _run_with_retries(self, existing_summary: str, proposed_summary: str):
        last_error: Exception | None = None
        for attempt in range(RETRY_ATTEMPTS + 1):
            try:
                return self._client.run_conflict_check(existing_summary, proposed_summary)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(BACKOFF_BASE_SECONDS * (2**attempt))
        raise last_error if last_error else RuntimeError("Conflict check failed")
