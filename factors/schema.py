import datetime
from dataclasses import dataclass, field
from typing import List, Optional
from data.sources.base import Event

@dataclass
class Observation:
    text: str                 
    asset: Optional[str] = None
    confidence: float = 1.0    
    tags: List[str] = field(default_factory=list)

@dataclass
class TextualFactor:
    date: datetime
    agent_name: str
    observations: List[Observation]
    length_tokens: int         
    preference: Optional[str] = None    
    raw_sources: List[Event] = field(default_factory=list)  