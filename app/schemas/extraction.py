from typing import List, Optional

from pydantic import BaseModel, Field


class Decision(BaseModel):
    title: str
    details: str
    owners: Optional[List[str]] = None
    due: Optional[str] = None


class ActionItem(BaseModel):
    task: str
    owner: Optional[str] = None
    due_date: Optional[str] = None


class Claim(BaseModel):
    statement: str
    type: str
    confidence: float = Field(ge=0.0, le=1.0)


class Extraction(BaseModel):
    participants: List[str]
    topics: List[str]
    decisions: List[Decision]
    action_items: List[ActionItem]
    claims: List[Claim]
