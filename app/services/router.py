import json
from dataclasses import dataclass
from typing import Dict, List, Tuple

from app.db.session import get_conn


MAX_STAKEHOLDERS = 10


@dataclass
class Stakeholder:
    person_id: int
    score: float
    reason: str
    mode: str


@dataclass
class RouteResult:
    pr_id: int
    stakeholders: int


def _normalize_scores(weights: Dict[int, float]) -> Dict[int, float]:
    if not weights:
        return {}
    max_weight = max(weights.values())
    if max_weight <= 0:
        return {k: 0.0 for k in weights}
    return {k: v / max_weight for k, v in weights.items()}


class Router:
    def route(self, pr_id: int) -> RouteResult:
        conn = get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT source_turn_id, extracted_json FROM knowledge_prs WHERE id = %s",
                    (pr_id,),
                )
                pr_row = cursor.fetchone()
                if not pr_row:
                    raise ValueError("PR not found")

                source_turn_id, extracted_json = pr_row
                topics = self._extract_topics(extracted_json)

                sender_person_id = None
                if source_turn_id:
                    cursor.execute(
                        "SELECT sender_person_id FROM turns WHERE id = %s",
                        (source_turn_id,),
                    )
                    row = cursor.fetchone()
                    if row:
                        sender_person_id = row[0]

                comm_weights = self._build_comm_edges(cursor, sender_person_id)
                proximity_scores = _normalize_scores(comm_weights)

                candidate_ids = set(proximity_scores.keys())
                if sender_person_id:
                    candidate_ids.add(sender_person_id)

                stakeholders: List[Stakeholder] = []
                for person_id in candidate_ids:
                    proximity = proximity_scores.get(person_id, 0.0)
                    topic_affinity = 1.0 if (topics and person_id == sender_person_id) else 0.0
                    score = 0.6 * proximity + 0.4 * topic_affinity
                    mode = self._mode_for_score(score)
                    reason_parts = []
                    if proximity > 0:
                        reason_parts.append("Direct comms link")
                    if topics and person_id == sender_person_id:
                        reason_parts.append(f"Topic match: {', '.join(topics[:3])}")
                    reason = "; ".join(reason_parts) or "No strong signals"
                    stakeholders.append(Stakeholder(person_id, score, reason, mode))

                stakeholders.sort(key=lambda s: s.score, reverse=True)
                stakeholders = stakeholders[:MAX_STAKEHOLDERS]

                cursor.execute("DELETE FROM pr_stakeholders WHERE pr_id = %s", (pr_id,))
                for stakeholder in stakeholders:
                    cursor.execute(
                        """
                        INSERT INTO pr_stakeholders (pr_id, person_id, score, reason, mode)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            pr_id,
                            stakeholder.person_id,
                            stakeholder.score,
                            stakeholder.reason,
                            stakeholder.mode,
                        ),
                    )

            conn.commit()
            return RouteResult(pr_id=pr_id, stakeholders=len(stakeholders))
        finally:
            conn.close()

    def _build_comm_edges(self, cursor, sender_person_id: int | None) -> Dict[int, float]:
        if not sender_person_id:
            return {}
        cursor.execute(
            """
            SELECT mr.recipient_person_id, COUNT(*)
            FROM messages m
            JOIN message_recipients mr ON mr.message_id = m.id
            WHERE m.sender_person_id = %s
            GROUP BY mr.recipient_person_id
            """,
            (sender_person_id,),
        )
        return {row[0]: float(row[1]) for row in cursor.fetchall()}

    def _extract_topics(self, extracted_json) -> List[str]:
        if extracted_json is None:
            return []
        data = extracted_json
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                return []
        topics = data.get("topics") if isinstance(data, dict) else None
        if not topics:
            return []
        return [str(t) for t in topics if t]

    def _mode_for_score(self, score: float) -> str:
        if score >= 0.7:
            return "notify_now"
        if score >= 0.4:
            return "digest"
        return "archive"
