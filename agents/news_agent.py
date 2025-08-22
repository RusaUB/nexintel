from __future__ import annotations
import logging
from typing import List, Optional
from datetime import datetime
import re

from clients.deepseek import DeepSeek  
from factors.schema import TextualFactor, Observation
from data.sources.base import Event
from agents.base import BaseAgent

SYMBOL_MAP = {
    r"\bbitcoin\b|\bbtc\b": "BTC",
    r"\beth(er|ereum)?\b|\beth\b": "ETH",
    r"\bsolana\b|\bsol\b": "SOL",
    r"\bbnb\b": "BNB",
    r"\bton\b|\btoncoin\b": "TON",
}

def guess_asset_simple(text: str) -> Optional[str]:
    t = (text or "").lower()
    for pat, sym in SYMBOL_MAP.items():
        if re.search(pat, t):
            return sym
    return None

def rough_token_len(*texts: str) -> int:
    words = sum(len((t or "").split()) for t in texts)
    return int(words * 1.3)


class NewsDataAgent(BaseAgent):
    """
    Takes the Event batch for the day → LLM (DeepSeek) performs intensive reading and
    returns 3–7 atomic observations → we collect TextualFactor (≤ ~4k tokens).
    Ready for Data Contest (Quantify→Predict→Allocate).
    """

    def __init__(
        self,
        name: str = "NewsDataAgent",
        preference: Optional[str] = None,
        max_obs: int = 7,
        max_tokens_factor: int = 4000,
        model: str = "deepseek-chat",
        sdk: Optional[DeepSeek] = None,
        llm_max_output_tokens: int = 1200,
    ):
        super().__init__(name)
        self.preference = preference
        self.max_obs = max_obs
        self.max_tokens_factor = max_tokens_factor
        self.sdk = sdk or DeepSeek(default_model=model)
        self.llm_max_output_tokens = llm_max_output_tokens
        self.logger = logging.getLogger(f"NexIntel.Agents.{self.__class__.__name__}.{self.name}")
        self.logger.debug(
            "NewsDataAgent init: pref=%s, max_obs=%d, max_tokens_factor=%d, model=%s",
            self.preference, self.max_obs, self.max_tokens_factor, model
        )


    def _filter_events(self, events: List[Event]) -> List[Event]:
        self.logger.debug("Filter: input_events=%d, preference=%s", len(events), self.preference)
        if not self.preference:
            return events
        pref = self.preference.lower()
        scored = []
        for ev in events:
            score = (f"{ev.title} {ev.content}".lower()).count(pref)
            scored.append((score, ev))
        scored.sort(key=lambda x: x[0], reverse=True)
        filtered = [ev for s, ev in scored if s > 0] or events
        self.logger.debug("Filter: output_events=%d", len(filtered))
        return filtered


    def _llm_extract_observations(self, date: datetime, events: List[Event]) -> List[Observation]:
        bullets = []
        for i, ev in enumerate(events, 1):
            bullets.append(f"{i}. {ev.title.strip()} :: {ev.content.strip()[:280]}")
        news_blob = "\n".join(bullets)

        system = (
            "You are a DataAgent in a multi-agent trading system. "
            "Task: collect a SHORT factor from 3–7 atomic observations in one day. "
            "Each observation is ONE asset and a brief explanation of "
            "why the event affects the price within a 1–3 day horizon. "
            "Symbols CANNOT be limited to a fixed list: if the asset is new, return it as is."
        )
        user = f"""Date: {date.date()}
Preference of the day: {self.preference or "нет"}

News (headline :: brief text):
{news_blob}

Form STRICTLY JSON:
{{
  "observations": [
    {{
      "text": "Short atomic observation (what happened → why it is important → asset)",
      "asset": "The main symbol in UPPERCASE or null if unsure",
      "symbols": [‘LIST of possible symbols/synonyms/tickers, can be empty’],
      "confidence": 0.0-1.0
    }}
  ]
}}
Rules:
- 3–7 observations; if there are several assets in one text, divide them into separate observations.
- Don't invent popular tickers, but if the asset is new/rare, still return it by name, as in the news (UPPERCASE).
- If a contract/address is specified, add a line in the format ‘CA:<address>’ to ‘symbols’.
- Keep it short (≤ ~4k tokens).
"""

        self.logger.debug("LLM call: events=%d, tokens_hint<=%d", len(events), self.llm_max_output_tokens)
        try:
            data = self.sdk.json_chat(
                messages=[{"role": "system", "content": system},
                          {"role": "user",   "content": user}],
                max_tokens=self.llm_max_output_tokens,
            )
        except Exception as e:
            self.logger.exception("LLM call failed, falling back: %s", e)
         
            return [
                Observation(
                    text=f"{ev.title.strip()}. May affect short-term supply/demand.",
                    asset=guess_asset_simple(f"{ev.title} {ev.content}"),
                    confidence=0.7,
                    tags=["news", "fallback"]
                )
                for ev in events
            ]

        raw = data.get("observations") or []
        self.logger.debug("LLM parsed observations: %d", len(raw))

        out: List[Observation] = []
        for i, item in enumerate(raw, 1):
            text = (item.get("text") or "").strip()
  
            asset = (item.get("asset") or "").strip() or None
            symbols = item.get("symbols") or []
            conf = float(item.get("confidence", 1.0) or 1.0)

            if not asset:
                asset = guess_asset_simple(text)

            if asset and asset not in {"BTC", "ETH", "SOL", "BNB", "TON"}:
                self.logger.info("LLM detected possibly new/unknown symbol: %s (symbols=%s)", asset, symbols)

            out.append(Observation(
                text=text,
                asset=asset,
                confidence=max(0.0, min(conf, 1.0)),
                tags=["news"]
            ))
        return out

    def _dedup_and_limit(self, observations: List[Observation]) -> List[Observation]:
        self.logger.debug("Dedup/limit: input_obs=%d", len(observations))
        seen = set(); kept: List[Observation] = []; total = 0
        for obs in observations:
            key = f"{obs.asset or 'NA'}|{obs.text}"
            if key in seen:
                continue
            seen.add(key)
            cand = rough_token_len(obs.text)
            if total + cand > self.max_tokens_factor:
                self.logger.debug("Token limit reached: total=%d, next=%d, limit=%d",
                                  total, cand, self.max_tokens_factor)
                break
            kept.append(obs); total += cand
            if len(kept) >= self.max_obs:
                self.logger.debug("max_obs reached: %d", self.max_obs)
                break
        self.logger.debug("Dedup/limit: output_obs=%d, tokens≈%d", len(kept), total)
        return kept

    def run(self, date: datetime, events: List[Event]) -> TextualFactor:
        self.logger.info("Run: date=%s, raw_events=%d", date.date(), len(events))
        filtered = self._filter_events(events)

        if not filtered:
            self.logger.info("No relevant events; emitting neutral factor")
            empty = Observation(text="No new significant events identified; neutral day.",
                                tags=["news"])
            factor = TextualFactor(date, self.name, [empty], rough_token_len(empty.text),
                                   self.preference, [])
            self.logger.debug("Factor built: obs=%d, tokens≈%d",
                              len(factor.observations), factor.length_tokens)
            return factor

        obs = self._llm_extract_observations(date, filtered)
        obs = self._dedup_and_limit(obs)
        length_tokens = sum(rough_token_len(o.text) for o in obs)

        factor = TextualFactor(date, self.name, obs, length_tokens, self.preference, filtered)
        self.logger.info("Factor built: obs=%d, tokens≈%d", len(obs), length_tokens)
        return factor