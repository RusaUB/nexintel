from base import BaseSource, Event
import requests
import datetime

class CoinDeskSource(BaseSource):
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url
    
    def connect(self):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    def fetch(self, start, end):
        params = {
            "lang": "EN",
            "categories": [],
            "limit" : 1,
            "source_ids": [],
            "exclude_categories": [],
            "to_ts": -1,
        }
        resp = self.session.get(self.base_url,params=params)
        resp.raise_for_status()
        resp_json = resp.json()
        return resp_json
    
    def normalize(self, raw):
        return Event(
            timestamp=datetime.datetime.now(),
            asset=None,
            source="CoinDesk",
            title=raw["Data"][0]["TITLE"],
            content=raw["Data"][0]["BODY"],
            sentiment=None,
            meta=None
        )
    
    def close(self):
        if self.session:
            self.session.close()


