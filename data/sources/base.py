from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

@dataclass
class Event:
    timestamp: datetime
    asset: Optional[str]
    source: str
    title: str
    content: str
    sentiment: Optional[float] = None
    meta: Dict = None


class BaseSource(ABC):
    @abstractmethod
    def connect(self):
        """Establish connection """
        pass
    
    @abstractmethod
    def fetch(self,start,end):
        """Download raw data for the period"""
        pass

    @abstractmethod
    def normalize(self,raw) -> Event:
        """Convert raw data to Event"""
        pass

    @abstractmethod
    def close(self):
        """Close connection"""
        pass