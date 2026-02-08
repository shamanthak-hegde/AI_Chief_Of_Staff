from __future__ import annotations

import time
from dataclasses import dataclass

from app.llm.openai_client import OpenAIClient
from app.schemas.extraction import Extraction
from app.services.cache import get_extraction_cache, set_extraction_cache


@dataclass
class ExtractionResult:
    extraction: Extraction
    truncated: bool


MAX_INPUT_CHARS = 12_000
RETRY_ATTEMPTS = 2
BACKOFF_BASE_SECONDS = 0.5


class ExtractorService:
    def __init__(self, client: OpenAIClient | None = None) -> None:
        self._client = client or OpenAIClient()

    def extract_turn(self, text: str, turn_id: int | None = None) -> ExtractionResult:
        cleaned = text.strip()
        if not cleaned:
            raise ValueError("Turn text is empty")

        truncated = False
        if len(cleaned) > MAX_INPUT_CHARS:
            cleaned = cleaned[:MAX_INPUT_CHARS]
            truncated = True

        if turn_id is not None:
            cached = get_extraction_cache(self._client.model_name, turn_id, cleaned)
            if cached:
                extraction = Extraction.model_validate(cached)
                return ExtractionResult(extraction=extraction, truncated=truncated)

        prompt = (
            "Extract structured updates from the following turn. "
            "Return participants, topics, decisions, action_items, and claims. "
            "Use concise strings and avoid hallucinating unknown values.\n\n"
            f"Turn:\n{cleaned}"
        )

        last_error: Exception | None = None
        for attempt in range(RETRY_ATTEMPTS + 1):
            try:
                extraction = self._client.run_extraction(prompt)
                if turn_id is not None:
                    set_extraction_cache(
                        self._client.model_name,
                        turn_id,
                        cleaned,
                        extraction.model_dump(),
                    )
                return ExtractionResult(extraction=extraction, truncated=truncated)
            except Exception as exc:  # noqa: BLE001 - surface after retries
                last_error = exc
                if attempt < RETRY_ATTEMPTS:
                    time.sleep(BACKOFF_BASE_SECONDS * (2**attempt))

        raise last_error if last_error else RuntimeError("Extraction failed")
