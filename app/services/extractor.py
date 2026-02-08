from __future__ import annotations

from dataclasses import dataclass

from app.llm.openai_client import OpenAIClient
from app.schemas.extraction import Extraction


@dataclass
class ExtractionResult:
    extraction: Extraction
    truncated: bool


MAX_INPUT_CHARS = 12_000
RETRY_ATTEMPTS = 2


class ExtractorService:
    def __init__(self, client: OpenAIClient | None = None) -> None:
        self._client = client or OpenAIClient()

    def extract_turn(self, text: str) -> ExtractionResult:
        cleaned = text.strip()
        if not cleaned:
            raise ValueError("Turn text is empty")

        truncated = False
        if len(cleaned) > MAX_INPUT_CHARS:
            cleaned = cleaned[:MAX_INPUT_CHARS]
            truncated = True

        prompt = (
            "Extract structured updates from the following turn. "
            "Return participants, topics, decisions, action_items, and claims. "
            "Use concise strings and avoid hallucinating unknown values.\n\n"
            f"Turn:\n{cleaned}"
        )

        last_error: Exception | None = None
        for _ in range(RETRY_ATTEMPTS + 1):
            try:
                extraction = self._client.run_extraction(prompt)
                return ExtractionResult(extraction=extraction, truncated=truncated)
            except Exception as exc:  # noqa: BLE001 - surface after retries
                last_error = exc

        raise last_error if last_error else RuntimeError("Extraction failed")
