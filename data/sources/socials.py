import os
import pickle
import time
from dune_client.client import DuneClient
from base import BaseSource, Event

CACHE_TTL = 60 * 60 * 12 # 12h

def build_mentions_text(rows, category="crypto"):
    positive_changes = []
    negative_changes = []

    for i in rows:
        if i["mention_growth"] > 0:
            positive_changes.append((i["symbol"], i["mention_growth"]))
        else:
            negative_changes.append((i["symbol"], i["mention_growth"]))

    text = ""

    if positive_changes:
        text += f"Over the past week, {category} mentions showed strong growth:\n"
        max_idx = min(5, len(positive_changes))
        info = ", ".join(
            [f"{positive_changes[j][0]} (+{round(positive_changes[j][1]*100)}%)"
             for j in range(max_idx)]
        )
        text += info + ".\n"

    if negative_changes:
        max_idx = min(5, len(negative_changes))
        info = ", ".join(
            [f"{negative_changes[j][0]} ({round(negative_changes[j][1]*100)}%)"
             for j in range(max_idx)]
        )
        text += info + " recorded declines, signaling reduced community interest.\n"

    return text


class DuneSocialSource(BaseSource):
    def __init__(self, api_key: str, cache_dir="dune_cache"):
        super().__init__(name="DuneSocialSource")
        self.api_key = api_key
        self.dune = None
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

        # query_id для layer1 и layer2
        self.query_ids = {
            "Layer 1": 3682694,
            "Layer 2": 3682730
        }

    def connect(self):
        self.dune = DuneClient(api_key=self.api_key)
        self.logger.info("Connected to Dune API")

    def fetch_query(self, query_id, label):
        cache_file = os.path.join(self.cache_dir, f"{label.replace(' ', '_')}.pkl")

        if os.path.exists(cache_file):
            mtime = os.path.getmtime(cache_file)  
            age = time.time() - mtime
            if age < CACHE_TTL:
                with open(cache_file, "rb") as f:
                    return pickle.load(f)
            else:
                self.logger.info(f"Cache for {label} expired ({age/3600:.1f}h old), refreshing...")


        query_result = self.dune.get_latest_result(query_id)
        with open(cache_file, "wb") as f:
            pickle.dump(query_result, f)
        return query_result

    def fetch(self, start=None, end=None):
        results = {}
        for label, qid in self.query_ids.items():
            raw = self.fetch_query(qid, label)
            results[label] = raw.result.rows
        return results

    def get_trends_report(self):
        results = self.fetch()
        reports = {}
        for label, rows in results.items():
            reports[label] = build_mentions_text(rows, category=label)
        return reports

    def get_combined_report(self):
        reports = self.get_trends_report()
        combined = ""
        if "Layer 1" in reports:
            combined += "Layer 1:\n" + reports["Layer 1"] + "\n"
        if "Layer 2" in reports:
            combined += "Layer 2:\n" + reports["Layer 2"] + "\n"
        return combined.strip()

    def normalize(self, raw):
        """
        Convert raw Layer1/Layer2 rows into Event objects.
        Returns a list of Event (one per Layer).
        """
        events = []

        for label, rows in raw.items():
            content = build_mentions_text(rows, category=label)
            event = Event(
                timestamp=None,
                asset=label,                          # "Layer 1" or "Layer 2"
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


from dotenv import load_dotenv
import os

load_dotenv()

dune_api_key = os.getenv("DUNE_API_KEY")

dune = DuneSocialSource(api_key=dune_api_key)
dune.connect()

raw_data = dune.fetch()
events = dune.normalize(raw_data)
print(events)