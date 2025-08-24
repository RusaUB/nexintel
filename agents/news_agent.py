from __future__ import annotations
import logging
import re
from typing import List, Optional, Dict, Any
from datetime import datetime

from clients.deepseek import DeepSeek
from factors.schema import TextualFactor, Observation
from data.sources.base import Event
from agents.base import BaseAgent

logger = logging.getLogger("NexIntel")

# --- asset fallback map (used only if LLM didn't return an asset) ---
SYMBOL_MAP = {
    r"\bbitcoin\b|\bbtc\b": "BTC",
    r"\beth(er|ereum)?\b|\beth\b": "ETH",
    r"\bsolana\b|\bsol\b": "SOL",
    r"\bbnb\b": "BNB",
    r"\bton\b|\btoncoin\b": "TON",
}

def _guess_asset_simple(text: str) -> Optional[str]:
    t = (text or "").lower()
    for pat, sym in SYMBOL_MAP.items():
        if re.search(pat, t):
            return sym
    return None

def _rough_token_len(*texts: str) -> int:
    words = sum(len((t or "").split()) for t in texts)
    return int(words * 1.3)

def _snake(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")

def _coerce_rating(x: Any, default: int = 0) -> int:
    """Map any value to an int in [-2,2]."""
    try:
        v = int(float(x))
    except Exception:
        v = default
    return max(-2, min(2, v))


class NewsDataAgent(BaseAgent):
    """
    DataAgent: reads a batch of Event objects for the day, asks DeepSeek to produce
    3–7 atomic observations (each linked to ONE asset), assigns topical tags,
    and builds a TextualFactor (<= ~4k tokens) ready for Data Contest.
    All tunables are loaded from YAML config via `from_config(...)`.
    """

    # ------- construction from config -------
    @classmethod
    def from_config(cls, agent_cfg: Dict[str, Any], secrets_cfg: Optional[Dict[str, Any]] = None) -> "NewsDataAgent":
        name = "NewsDataAgent"
        preference = agent_cfg.get("preference")
        max_obs = int(agent_cfg.get("max_obs", 7))
        max_tokens_factor = int(agent_cfg.get("max_tokens_factor", 4000))

        ds = agent_cfg.get("deepseek", {}) or {}
        model = ds.get("model", "deepseek-chat")
        base_url = ds.get("base_url", "https://api.deepseek.com")
        max_output_tokens = int(ds.get("max_output_tokens", 1200))

        sdk = DeepSeek(
            default_model=model,
            base_url=base_url,
        )

        tags_cfg = (agent_cfg.get("tags") or {})
        llm_assign_tags = bool(tags_cfg.get("llm_assign_tags", True))
        max_tags_per_observation = int(tags_cfg.get("max_tags_per_observation", 3))
        tag_canon = set(tags_cfg.get("canon") or [])
        tag_synonyms = dict(tags_cfg.get("synonyms") or {})

        inst = cls(
            name=name,
            preference=preference,
            max_obs=max_obs,
            max_tokens_factor=max_tokens_factor,
            model=model,
            sdk=sdk,
            llm_max_output_tokens=max_output_tokens,
        )
        inst._llm_assign_tags = llm_assign_tags
        inst._max_tags_per_obs = max_tags_per_observation
        inst._tag_canon = tag_canon
        inst._tag_synonyms = tag_synonyms

        inst.logger.info(
            "Configured from YAML: pref=%s max_obs=%d L_tokens=%d model=%s base_url=%s llm_tags=%s max_tags=%d",
            preference, max_obs, max_tokens_factor, model, base_url, llm_assign_tags, max_tags_per_observation
        )
        return inst

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
        self.sdk = sdk or DeepSeek(default_model=model, response_format="json_object")
        self.llm_max_output_tokens = llm_max_output_tokens

        self._llm_assign_tags: bool = True
        self._max_tags_per_obs: int = 3
        self._tag_canon: set[str] = set()
        self._tag_synonyms: Dict[str, str] = {}

        self.logger = logging.getLogger(f"NexIntel.Agents.{self.__class__.__name__}.{self.name}")
        self.logger.debug(
            "NewsDataAgent init: pref=%s, max_obs=%d, max_tokens_factor=%d",
            self.preference, self.max_obs, self.max_tokens_factor
        )

    # ------- tagging helpers -------
    def _normalize_tags(self, raw_tags: List[str], text: str) -> List[str]:
        """
        Normalize tags with config:
          1) snake_case, 2) apply synonyms->canon map, 3) deduplicate,
          4) cap by max_tags_per_observation, 5) heuristic fallback if empty.
        """
        seen = set()
        out: List[str] = []

        for tag in raw_tags or []:
            t0 = _snake(tag)
            if not t0:
                continue
            t1 = self._tag_synonyms.get(t0, t0)
            if t1 not in seen:
                seen.add(t1)
                out.append(t1)

        if len(out) < 1:
            txt = (text or "").lower()

            def add(tag: str):
                if tag not in seen:
                    seen.add(tag)
                    out.append(tag)

            if any(k in txt for k in ["cpi", "fed", "yield", "rates", "inflation", "macro"]):
                add("macro")
            if any(k in txt for k in ["etf", "blackrock", "fidelity", "inflows", "outflows"]):
                add("etf")
            if any(k in txt for k in ["on-chain", "onchain", "addresses", "tvl", "bridge", "staking"]):
                add("onchain")
            if any(k in txt for k in ["funding", "perp", "perps", "basis", "oi", "open interest", "liquidations"]):
                add("derivatives")
            if any(k in txt for k in ["orderbook", "order book", "bid wall", "ask wall", "liquidity wall", "depth"]):
                add("orderbook")
            if any(k in txt for k in ["unlock", "emission", "halving", "supply schedule"]):
                add("tokenomics")
            if any(k in txt for k in ["stablecoin", "usdt", "usdc", "stable flow"]):
                add("stablecoins")
            if any(k in txt for k in ["dex", "amm", "lp", "pool"]):
                add("dex")
            if any(k in txt for k in ["cex", "binance", "bybit", "okx", "kraken", "coinbase"]):
                add("cex")
            if any(k in txt for k in ["narrative", "sector rotation", "theme"]):
                add("narratives")

        out = out[: self._max_tags_per_obs]

        for t in out:
            if self._tag_canon and t not in self._tag_canon:
                self.logger.info("Tags: detected NEW/NON-CANON tag: %s", t)

        return out

    # ------- pipeline steps -------
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
        # Build compact news blob
        bullets = []
        for i, ev in enumerate(events, 1):
            bullets.append(f"{i}. {ev.title.strip()} :: {ev.content.strip()[:280]}")
        news_blob = "\n".join(bullets)

        # Prompt: ask for discrete rating in [-2..2]
        system = (
            "You are a DataAgent in a multi-agent trading system. "
            "Produce a SHORT textual factor with 3–7 atomic observations for the day. "
            "Each observation MUST focus on ONE asset, explain the 1–3 day price impact, "
            "and include a discrete impact rating in {-2,-1,0,1,2} (direction & strength). "
        )
        if self._llm_assign_tags:
            system += "Additionally, assign smart topical tags per observation."

        if self._llm_assign_tags:
            user = f"""Date: {date.date()}
Preference of the day: {self.preference or "none"}

News (headline :: brief):
{news_blob}

Return STRICT JSON:
{{
  "observations": [
    {{
      "text": "Short atomic observation (what happened → why it matters → asset)",
      "asset": "MAIN SYMBOL in UPPERCASE or null if unsure",
      "symbols": ["LIST of possible symbols/aliases/tickers, can be empty"],
      "rating": -2,  # integer in [-2,-1,0,1,2]
      "tags": ["lower_snake_case topical tags"]
    }}
  ]
}}
Rules:
- 3–7 observations; if a sentence mentions multiple assets, split into separate observations.
- DO NOT restrict symbols to a predefined list: if the asset is new/rare, still return it as-is (UPPERCASE). Contract/address may be noted as "CA:<address>" in symbols.
- Tags MUST be topical (not assets), in lower_snake_case. Create a new tag if necessary (keep it short and general).
- Keep it short (overall ≤ ~4k tokens).
"""
        else:
            user = f"""Date: {date.date()}
Preference of the day: {self.preference or "none"}

News (headline :: brief):
{news_blob}

Return STRICT JSON:
{{
  "observations": [
    {{
      "text": "Short atomic observation (what happened → why it matters → asset)",
      "asset": "MAIN SYMBOL in UPPERCASE or null if unsure",
      "symbols": ["LIST of possible symbols/aliases/tickers, can be empty"],
      "rating": 0   # integer in [-2,-1,0,1,2]
    }}
  ]
}}
Rules:
- 3–7 observations; if a sentence mentions multiple assets, split into separate observations.
- DO NOT restrict symbols to a predefined list: if the asset is new/rare, still return it as-is (UPPERCASE). Contract/address may be noted as "CA:<address>" in symbols.
- Keep it short (overall ≤ ~4k tokens).
"""

        self.logger.debug("LLM call: events=%d, tokens_hint<=%d (tags_from_llm=%s)",
                          len(events), self.llm_max_output_tokens, self._llm_assign_tags)
        try:
            data = self.sdk.json_chat(
                messages=[{"role": "system", "content": system},
                          {"role": "user",   "content": user}],
                max_tokens=self.llm_max_output_tokens,
            )
        except Exception as e:
            self.logger.exception("LLM call failed, falling back: %s", e)
            # Simple fallback: build one neutral observation per event
            return [
                Observation(
                    text=f"{ev.title.strip()}. May affect short-term supply/demand.",
                    asset=_guess_asset_simple(f"{ev.title} {ev.content}"),
                    rating=0,
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
            llm_tags = item.get("tags") or []
            rating = _coerce_rating(item.get("rating", 0), default=0)

            if not asset:
                asset = _guess_asset_simple(text)

            if asset and asset not in {"BTC", "ETH", "SOL", "BNB", "TON"}:
                self.logger.info("LLM detected possibly new/unknown symbol: %s (symbols=%s)", asset, symbols)

            tags = self._normalize_tags([str(t) for t in llm_tags], text=text)
            self.logger.debug("Obs %d: asset=%s rating=%+d tags=%s", i, asset, rating, tags)

            out.append(Observation(
                text=text,
                asset=asset,
                rating=rating,
                tags=tags or ["news"]
            ))
        return out

    def _dedup_and_limit(self, observations: List[Observation]) -> List[Observation]:
        self.logger.debug("Dedup/limit: input_obs=%d", len(observations))
        seen = set()
        kept: List[Observation] = []
        total = 0
        for obs in observations:
            key = f"{obs.asset or 'NA'}|{obs.text}"
            if key in seen:
                continue
            seen.add(key)
            cand = _rough_token_len(obs.text)
            if total + cand > self.max_tokens_factor:
                self.logger.debug("Token limit reached: total=%d, next=%d, limit=%d",
                                  total, cand, self.max_tokens_factor)
                break
            kept.append(obs)
            total += cand
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
                                rating=0, tags=["news"])
            factor = TextualFactor(date, self.name, [empty], _rough_token_len(empty.text),
                                   self.preference, [])
            self.logger.debug("Factor built: obs=%d, tokens≈%d",
                              len(factor.observations), factor.length_tokens)
            return factor

        obs = self._llm_extract_observations(date, filtered)
        obs = self._dedup_and_limit(obs)
        length_tokens = sum(_rough_token_len(o.text) for o in obs)

        factor = TextualFactor(date, self.name, obs, length_tokens, self.preference, filtered)
        self.logger.info("Factor built: obs=%d, tokens≈%d", len(obs), length_tokens)
        return factor