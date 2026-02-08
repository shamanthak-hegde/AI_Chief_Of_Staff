from typing import Optional

from pydantic import BaseModel


class ConflictCheck(BaseModel):
    conflict: bool
    conflict_type: str
    existing_span: Optional[str] = None
    new_span: Optional[str] = None
    resolution_hint: Optional[str] = None
