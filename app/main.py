import os

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.db.init_db import init_db
from app.db.session import get_conn
from app.ingest.enron import ingest_enron
from app.ingest.slack import ingest_slack
from app.services.conflict_detector import ConflictDetector
from app.services.extractor import ExtractorService
from app.services.kpr_builder import KPRBuilder
from app.services.router import Router


settings = get_settings()
setup_logging(settings.log_level)

app = FastAPI(title=settings.app_name)
extractor_service = ExtractorService()
kpr_builder = KPRBuilder(extractor=extractor_service)
conflict_detector = ConflictDetector()
router = Router()


class EnronIngestRequest(BaseModel):
    path: str
    limit: int = 0


class SlackIngestRequest(BaseModel):
    path: str
    limit: int = 0


@app.on_event("startup")
def ensure_storage() -> None:
    os.makedirs("data", exist_ok=True)
    init_db()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/ingest/enron")
def ingest_enron_endpoint(payload: EnronIngestRequest) -> dict:
    return ingest_enron(payload.path, payload.limit)


@app.post("/ingest/slack")
def ingest_slack_endpoint(payload: SlackIngestRequest) -> dict:
    return ingest_slack(payload.path, payload.limit)


@app.post("/analyze/turn/{turn_id}")
def analyze_turn(turn_id: int, response: Response) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT text FROM turns WHERE id = %s", (turn_id,))
            row = cursor.fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Turn not found")

    turn_text = row[0]
    try:
        result = extractor_service.extract_turn(turn_text, turn_id=turn_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="OpenAI extraction failed") from exc

    if result.truncated:
        response.headers["X-Input-Truncated"] = "true"

    return result.extraction.model_dump()


@app.post("/kpr/from_turn/{turn_id}")
def kpr_from_turn(turn_id: int) -> dict:
    try:
        result = kpr_builder.build_from_turn(turn_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="KPR build failed") from exc

    return {"pr_id": result.pr_id, "changes": result.changes}


@app.get("/kpr/{pr_id}")
def get_kpr(pr_id: int) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, created_at, source_turn_id, status, extracted_json, model, title
                FROM knowledge_prs
                WHERE id = %s
                """,
                (pr_id,),
            )
            pr_row = cursor.fetchone()
            if not pr_row:
                raise HTTPException(status_code=404, detail="PR not found")

            cursor.execute(
                """
                SELECT id, truth_item_id, previous_version_id, proposed_summary, diff_summary, similarity
                FROM pr_changes
                WHERE pr_id = %s
                ORDER BY id
                """,
                (pr_id,),
            )
            changes = [
                {
                    "id": row[0],
                    "truth_item_id": row[1],
                    "previous_version_id": row[2],
                    "proposed_summary": row[3],
                    "diff_summary": row[4],
                    "similarity": row[5],
                }
                for row in cursor.fetchall()
            ]
    finally:
        conn.close()

    return {
        "id": pr_row[0],
        "created_at": pr_row[1],
        "source_turn_id": pr_row[2],
        "status": pr_row[3],
        "extracted_json": pr_row[4],
        "model": pr_row[5],
        "title": pr_row[6],
        "changes": changes,
    }


@app.post("/kpr/{pr_id}/run_conflicts")
def run_conflicts(pr_id: int) -> dict:
    try:
        result = conflict_detector.run_conflicts(pr_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="Conflict detection failed") from exc

    return {"pr_id": result.pr_id, "conflicts": result.conflicts, "status": result.status}


@app.post("/kpr/{pr_id}/route")
def route_pr(pr_id: int) -> dict:
    try:
        result = router.route(pr_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="Routing failed") from exc

    return {"pr_id": result.pr_id, "stakeholders": result.stakeholders}


@app.get("/kpr/{pr_id}/stakeholders")
def get_stakeholders(pr_id: int) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM knowledge_prs WHERE id = %s",
                (pr_id,),
            )
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="PR not found")

            cursor.execute(
                """
                SELECT person_id, score, reason, mode
                FROM pr_stakeholders
                WHERE pr_id = %s
                ORDER BY score DESC
                """,
                (pr_id,),
            )
            stakeholders = [
                {
                    "person_id": row[0],
                    "score": row[1],
                    "reason": row[2],
                    "mode": row[3],
                }
                for row in cursor.fetchall()
            ]
    finally:
        conn.close()

    return {"pr_id": pr_id, "stakeholders": stakeholders}


@app.get("/graph/comms")
def graph_comms() -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, handle, display_name FROM people ORDER BY id")
            nodes = [
                {
                    "id": row[0],
                    "label": row[2] or row[1] or f"person-{row[0]}",
                    "type": "person",
                }
                for row in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT m.sender_person_id, mr.recipient_person_id, COUNT(*)
                FROM messages m
                JOIN message_recipients mr ON mr.message_id = m.id
                WHERE m.sender_person_id IS NOT NULL
                GROUP BY m.sender_person_id, mr.recipient_person_id
                """
            )
            edges = [
                {
                    "source": row[0],
                    "target": row[1],
                    "weight": row[2],
                }
                for row in cursor.fetchall()
            ]
    finally:
        conn.close()

    return {"nodes": nodes, "edges": edges}


@app.get("/graph/knowledge")
def graph_knowledge() -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, title FROM truth_items ORDER BY id")
            nodes = [
                {
                    "id": row[0],
                    "label": row[1],
                    "type": "truth_item",
                }
                for row in cursor.fetchall()
            ]

            cursor.execute(
                """
                SELECT pr_id, truth_item_id
                FROM pr_changes
                ORDER BY id
                """
            )
            edges = [
                {
                    "source": row[0],
                    "target": row[1],
                    "type": "pr_change",
                }
                for row in cursor.fetchall()
            ]
    finally:
        conn.close()

    return {"nodes": nodes, "edges": edges}


@app.get("/kpr/{pr_id}/trace")
def kpr_trace(pr_id: int) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT extracted_json, status FROM knowledge_prs WHERE id = %s",
                (pr_id,),
            )
            pr_row = cursor.fetchone()
            if not pr_row:
                raise HTTPException(status_code=404, detail="PR not found")
            extracted_json, pr_status = pr_row

            cursor.execute("SELECT COUNT(*) FROM pr_changes WHERE pr_id = %s", (pr_id,))
            change_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM pr_conflicts WHERE pr_id = %s", (pr_id,))
            conflict_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM pr_stakeholders WHERE pr_id = %s", (pr_id,))
            stakeholder_count = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) FROM truth_versions WHERE merged_from_pr_id = %s",
                (pr_id,),
            )
            merged_count = cursor.fetchone()[0]
    finally:
        conn.close()

    steps = []

    steps.append(
        {
            "key": "extractor",
            "label": "Extractor",
            "status": "complete" if extracted_json else "pending",
            "details": {"extracted": bool(extracted_json)},
        }
    )

    steps.append(
        {
            "key": "matcher",
            "label": "Matcher",
            "status": "complete" if change_count > 0 else "pending",
            "details": {"changes": change_count},
        }
    )

    critic_complete = conflict_count > 0 or pr_status in {"merge_conflict", "needs_review"}
    steps.append(
        {
            "key": "critic",
            "label": "Critic",
            "status": "complete" if critic_complete else "pending",
            "details": {"conflicts": conflict_count, "status": pr_status},
        }
    )

    steps.append(
        {
            "key": "router",
            "label": "Router",
            "status": "complete" if stakeholder_count > 0 else "pending",
            "details": {"stakeholders": stakeholder_count},
        }
    )

    steps.append(
        {
            "key": "merge",
            "label": "Merge",
            "status": "complete" if merged_count > 0 else "pending",
            "details": {"merged_versions": merged_count},
        }
    )

    return {"pr_id": pr_id, "steps": steps}
