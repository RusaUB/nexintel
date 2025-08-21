import os
from dune_client.client import DuneClient
from base import BaseSource, Event
from utils import ttl_cache, build_mentions_text

class DuneSocialSource(BaseSource):
    def __init__(self, api_key: str):
        super().__init__(name="DuneSocialSource")
        self.api_key = api_key
        self.dune = None

        self.query_ids = {
            "Layer 1": 3682694,
            "Layer 2": 3682730
        }
        self.cache_ns = f"DuneSocial:{sorted(self.query_ids.items())}"

    def connect(self):
        self.dune = DuneClient(api_key=self.api_key)
        self.logger.info("Connected to Dune API")

    @ttl_cache(ttl_seconds=60*60*12, cache_dir="dune_cache")
    def fetch(self, start=None, end=None):
        results = {}
        for label, qid in self.query_ids.items():
            raw = self.dune.get_latest_result(qid)
            results[label] = raw.result.rows
        return results

    def normalize(self, raw):
        events = []
        for label, rows in raw.items():
            content = build_mentions_text(rows, category=label)
            event = Event(
                timestamp=None,
                asset=label,
                source="Dune",
                title=f"Weekly {label} Mentions Report",
                content=content,
                sentiment=None,
                meta={"query_id": self.query_ids[label]}
            )
            events.append(event)
        return events

    def close(self):
        self.logger.info("Closed DuneSource connection")