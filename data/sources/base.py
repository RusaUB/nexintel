import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

logger = logging.getLogger("NexIntel")
logger.setLevel(logging.DEBUG) 

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    fmt="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
ch.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(ch)

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
    def __init__(self,name: str):
        self.name = name
        self.logger = logging.getLogger(f"NexIntel.{self.name}")

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