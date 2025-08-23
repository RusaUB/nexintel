from __future__ import annotations
import os
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
import requests

from data.sources.base import BaseSource, Event

Number = Union[int, float]

class CoinDeskSource(BaseSource):
    def __init__(
        self,
        api_key: Optional[str],
        base_url: str,
        fetch_defaults: Optional[Dict[str, Any]] = None,
        timeout_sec: int = 20,
    ):
        super().__init__(name="CoinDeskSource")
        self.api_key = api_key
        self.base_url = base_url
        self.fetch_defaults = fetch_defaults or {}
        self.timeout_sec = int(timeout_sec)
        self.session: Optional[requests.Session] = None

        self.fetch_defaults.setdefault("lang", "EN")
        self.fetch_defaults.setdefault("categories", [])
        self.fetch_defaults.setdefault("exclude_categories", [])
        self.fetch_defaults.setdefault("source_ids", [])
        self.fetch_defaults.setdefault("limit", 10)
        self.fetch_defaults.setdefault("to_ts", -1)

        self.logger.debug(
            "Init CoinDeskSource: base_url=%s timeout=%s fetch_defaults=%s",
            self.base_url, self.timeout_sec, {k: self.fetch_defaults.get(k) for k in ["lang","limit","to_ts"]}
        )

    @classmethod
    def from_config(cls, ds_cfg: Dict[str, Any], secrets_cfg: Optional[Dict[str, Any]] = None) -> "CoinDeskSource":
        """
        ds_cfg: config['data_sources']['coindesk']
        secrets_cfg: config['secrets'] (для имени переменной окружения с API-ключом)
        """
        secrets_cfg = secrets_cfg or {}
        api_env_name = secrets_cfg.get("coindesk_api_key_env", "COINDESK_API_KEY")
        api_key = os.getenv(api_env_name)

        base_url = ds_cfg.get("base_url")
        if not base_url:
            raise ValueError("coindesk.base_url must be set in config")

        fetch_defaults = ds_cfg.get("fetch", {}) or {}
        timeout_sec = int(ds_cfg.get("timeout_sec", 20))

        inst = cls(
            api_key=api_key,
            base_url=base_url,
            fetch_defaults=fetch_defaults,
            timeout_sec=timeout_sec,
        )
        return inst


    def connect(self):
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})
        self.logger.info("Connected to CoinDesk API (timeout=%ss)", self.timeout_sec)

    def close(self):
        if self.session:
            self.session.close()
            self.session = None
            self.logger.info("Session closed")


    @staticmethod
    def _as_unix_ts(x: Optional[Union[datetime, Number, str]]) -> Optional[int]:
        if x is None:
            return None
        if isinstance(x, datetime):
            return int(x.timestamp())
        if isinstance(x, (int, float)):
            return int(x)

        try:
            return int(float(x))
        except Exception:
            return None

    def _build_params(self, start: Optional[Union[datetime, Number, str]], end: Optional[Union[datetime, Number, str]]) -> Dict[str, Any]:

        params = {
            "lang": self.fetch_defaults.get("lang", "EN"),
            "categories": self.fetch_defaults.get("categories", []),
            "exclude_categories": self.fetch_defaults.get("exclude_categories", []),
            "source_ids": self.fetch_defaults.get("source_ids", []),
            "limit": self.fetch_defaults.get("limit", 10),
            "to_ts": self.fetch_defaults.get("to_ts", -1),
        }


        from_ts = self._as_unix_ts(start)
        to_ts = self._as_unix_ts(end)

        if from_ts is not None:
            params["from_ts"] = from_ts
        if to_ts is not None:
            params["to_ts"] = to_ts

        return params

 
    def fetch(self, start: Optional[Union[datetime, Number, str]], end: Optional[Union[datetime, Number, str]]):
        if not self.session:
            raise RuntimeError("Call connect() before fetch()")

        params = self._build_params(start, end)
        self.logger.debug("Fetching: url=%s params=%s", self.base_url, params)

        try:
            resp = self.session.get(self.base_url, params=params, timeout=self.timeout_sec)
            resp.raise_for_status()
            self.logger.debug("Fetched OK: status=%s items≈?", resp.status_code)
            return resp.json()
        except requests.HTTPError as e:
            body = (e.response.text if hasattr(e, "response") and e.response is not None else "")[:400]
            self.logger.error("HTTP error: %s body=%s", e, body)
            raise
        except requests.RequestException as e:
            self.logger.error("Request error: %s", e)
            raise

    def normalize(self, raw: Dict[str, Any]) -> List[Event]:
        """
        Приводим к Event. Если API меняет схему — скорректируй поля ниже.
        """
        try:
            events: List[Event] = []
            now = datetime.now()

            data = raw.get("Data") or raw.get("data") or []
            for item in data:

                title = item.get("TITLE") or item.get("title") or "No title"
                body = item.get("BODY") or item.get("summary") or item.get("content") or "No content"

                ev = Event(
                    timestamp=now,        
                    asset=None,
                    source="CoinDesk",
                    title=title,
                    content=body,
                    sentiment=None,
                    meta={
                        "id": item.get("ID") or item.get("id"),
                        "url": item.get("URL") or item.get("url"),
                        "categories": item.get("CATEGORIES") or item.get("categories"),
                    },
                )
                events.append(ev)

            self.logger.debug("Normalized %d events from CoinDesk", len(events))
            return events
        except Exception as e:
            self.logger.error("Error normalizing data: %s", e)
            raise