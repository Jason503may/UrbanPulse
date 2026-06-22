from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime

from schemas.location import Location


class UrbanPulseEvent(BaseModel):
    event_id: str
    event_type: str
    event_timestamp: datetime
    city: str
    source: str
    location: Location
    payload: Dict[str, Any]


