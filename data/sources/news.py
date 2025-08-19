from base import BaseSource, Event
import requests
import datetime

class CoinDeskSource(BaseSource):
    def __init__(self, api_key, base_url):
        super().__init__(name="CoinDeskSource")
        self.api_key = api_key
        self.base_url = base_url
        self.session = None
    
    def connect(self):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
        self.logger.info("Connected to CoinDesk API")

    def fetch(self, start, end):
        params = {
            "lang": "EN",
            "categories": [],
            "limit" : 1,
            "source_ids": [],
            "exclude_categories": [],
        }
        if end:
            params["to_ts"] = end
        else :
            params["to_ts"] = -1
        try:
            resp = self.session.get(self.base_url, params=params)
            resp.raise_for_status()
            self.logger.debug(f"Fetched data with params={params}")
            return resp.json()
        except Exception as e:
            self.logger.error(f"Error while fetching: {e}")
            raise
    
    def normalize(self, raw):
        try:
            event = Event(
                timestamp=datetime.datetime.now(),
                asset=None,
                source="CoinDesk",
                title=raw["Data"][0]["TITLE"],
                content=raw["Data"][0]["BODY"],
            )
            self.logger.debug(f"Normalized event: {event.title}")
            return event
        except Exception as e:
            self.logger.error(f"Error normalizing data: {e}")
            raise
    
    def close(self):
        if self.session:
            self.session.close()
            self.logger.info("Session closed")


