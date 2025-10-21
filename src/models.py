from pydantic import BaseModel, Field, validator
from typing import Any, Dict, List
from datetime import datetime

class Event(BaseModel):
    topic: str = Field(..., min_length=1)
    event_id: str = Field(..., min_length=1)
    timestamp: datetime
    source: str = Field(..., min_length=1)
    payload: Dict[str, Any]

    @validator("timestamp", pre=True)
    def parse_timestamp(cls, v):
        return v

class PublishResponse(BaseModel):
    received: int
    enqueued: int

class EventView(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

class Stats(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime_seconds: int
