from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class PersonRead(BaseModel):
    id: int
    handle: str
    display_name: Optional[str] = None
    email: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class MessageRead(BaseModel):
    id: int
    platform: str
    external_id: Optional[str] = None
    ts: datetime
    sender_person_id: Optional[int] = None
    channel_id: Optional[str] = None
    thread_id: Optional[str] = None
    subject: Optional[str] = None
    text: Optional[str] = None

    class Config:
        from_attributes = True


class TurnRead(BaseModel):
    id: int
    platform: str
    channel_id: Optional[str] = None
    thread_id: Optional[str] = None
    sender_person_id: Optional[int] = None
    start_ts: datetime
    end_ts: datetime
    text: str

    class Config:
        from_attributes = True


class TruthItemRead(BaseModel):
    id: int
    type: str
    title: str
    created_at: datetime
    current_version_id: Optional[int] = None

    class Config:
        from_attributes = True


class TruthVersionRead(BaseModel):
    id: int
    truth_item_id: int
    version_num: int
    created_at: datetime
    summary: str
    confidence: Optional[float] = None
    merged_from_pr_id: Optional[int] = None

    class Config:
        from_attributes = True


class KnowledgePRRead(BaseModel):
    id: int
    created_at: datetime
    source_turn_id: Optional[int] = None
    status: str
    model: Optional[str] = None
    title: Optional[str] = None

    class Config:
        from_attributes = True
