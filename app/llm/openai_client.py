from __future__ import annotations

from typing import Optional

from openai import OpenAI

from app.core.config import get_settings
from app.schemas.conflict import ConflictCheck
from app.schemas.extraction import Extraction


class OpenAIClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        settings = get_settings()
        self._client = OpenAI(
            api_key=api_key or settings.openai_api_key,
            timeout=timeout_seconds or settings.openai_timeout_seconds,
        )
        self._model = model or settings.openai_model
        self._embeddings_model = settings.embeddings_model

    def run_extraction(self, prompt_text: str) -> Extraction:
        response = self._client.responses.parse(
            model=self._model,
            input=[
                {
                    "role": "system",
                    "content": (
                        "You are an information extraction system. "
                        "Return only valid JSON that matches the provided schema."
                    ),
                },
                {"role": "user", "content": prompt_text},
            ],
            text_format=Extraction,
        )

        parsed = response.output_parsed
        if parsed is None:
            raise ValueError("Model response did not parse to Extraction schema")
        return parsed

    def embed(self, text: str) -> list[float]:
        response = self._client.embeddings.create(
            model=self._embeddings_model,
            input=text,
        )
        return response.data[0].embedding

    def run_conflict_check(self, existing_summary: str, proposed_summary: str) -> ConflictCheck:
        prompt = (
            "Compare the existing summary with the proposed summary. "
            "Determine if they conflict. If conflict exists, provide short conflicting spans "
            "from both summaries and a conflict_type such as direct_contradiction, staleness, "
            "or scope_mismatch. Return valid JSON matching the schema."
            f"\n\nExisting summary:\n{existing_summary}\n\nProposed summary:\n{proposed_summary}"
        )
        response = self._client.responses.parse(
            model=self._model,
            input=[
                {
                    "role": "system",
                    "content": (
                        "You are a conflict detection system. "
                        "Return only valid JSON that matches the provided schema."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            text_format=ConflictCheck,
        )
        parsed = response.output_parsed
        if parsed is None:
            raise ValueError("Model response did not parse to ConflictCheck schema")
        return parsed

    @property
    def model_name(self) -> str:
        return self._model
