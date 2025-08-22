import logging
from abc import ABC, abstractmethod
from typing import List
from datetime import datetime

from data.sources.base import Event
from factors.schema import TextualFactor

class BaseAgent(ABC):
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"NexIntel.Agents.{self.__class__.__name__}")
        self.logger.debug(f"Initialized agent: name={self.name}")

    @abstractmethod
    def run(self, date: datetime, events: List[Event]) -> TextualFactor:
        """Process a batch of events for a given date into a TextualFactor."""
        raise NotImplementedError