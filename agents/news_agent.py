from __future__ import annotations
import logging
from typing import List, Optional
from datetime import datetime
import re

from clients.deepseek import DeepSeek 
from factors.schema import TextualFactor, Observation
from data.sources.base import Event
from agents.base import BaseAgent

logger = logging.getLogger("NexIntel")

SYMBOL_MAP = {
    r"\bbitcoin\b|\bbtc\b": "BTC",
    r"\beth(er|ereum)?\b|\beth\b": "ETH",
    r"\bsolana\b|\bsol\b": "SOL",
    r"\bbnb\b": "BNB",
    r"\bton\b|\btoncoin\b": "TON",
}


CANON_TAGS = {
    "macro",
    "regulation",
    "etf",
    "onchain",
    "derivatives",
    "orderbook",
    "sentiment",
    "tokenomics",
    "stablecoins",
    "ecosystem",
    "protocol",
    "narratives",
    "cex",
    "dex",
    "liquidity",
}


TAG_SYNONYMS = {
    # macro
    "macro_economy": "macro", "macro_economic": "macro", "rates": "macro", "cpi": "macro",
    "fed": "macro", "yields": "macro",
    # regulation
    "regulatory": "regulation", "policy": "regulation", "sec": "regulation",
    # etf
    "etf_flows": "etf", "spot_etf": "etf", "futures_etf": "etf",
    # onchain
    "on_chain": "onchain", "chain": "onchain", "addresses": "onchain", "tvl": "onchain",
    # derivatives
    "perps": "derivatives", "funding": "derivatives", "open_interest": "derivatives",
    "basis": "derivatives", "liquidations": "derivatives",
    # orderbook
    "order_flow": "orderbook", "book": "orderbook", "depth": "orderbook",
    # sentiment
    "newsflow": "sentiment", "social": "sentiment", "tone": "sentiment",
    # tokenomics
    "unlock": "tokenomics", "emission": "tokenomics", "halving": "tokenomics",
    # stablecoins
    "stables": "stablecoins", "usdt": "stablecoins", "usdc": "stablecoins",
    # ecosystem/protocol/narratives
    "dev_activity": "protocol", "revenue": "protocol", "fee": "protocol",
    "l2": "ecosystem", "sector_rotation": "narratives",
    # venues
    "centralized_exchange": "cex", "decentralized_exchange": "dex",
    # liquidity
    "market_liquidity": "liquidity", "depth_liquidity": "liquidity",
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

def _snake(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")

def normalize_tags(raw_tags: List[str], text: str, max_tags: int = 3) -> List[str]:
    """
    1) приведение к snake_case, 2) канонизация синонимов, 3) отсев дубликатов,
    4) ограничение по количеству, 5) fallback-эвристика по ключевым словам текста.
    """
    seen = set()
    out: List[str] = []


    for tag in raw_tags or []:
        t0 = _snake(tag)
        if not t0:
            continue
        t1 = TAG_SYNONYMS.get(t0, t0)
        if t1 not in seen:
            seen.add(t1); out.append(t1)


    if len(out) < 1:
        txt = (text or "").lower()

        def add(tag: str):
            if tag not in seen:
                seen.add(tag); out.append(tag)

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

  
    out = out[:max_tags]


    for t in out:
        if t not in CANON_TAGS:
            logger.info("Tags: detected NEW/NON-CANON tag: %s", t)

    return out


class NewsDataAgent(BaseAgent):
    """
    Берёт батч Event за день → LLM (DeepSeek) делает интенсивное чтение и
    возвращает 3–7 атомарных наблюдений с УМНЫМИ тегами → собираем TextualFactor (≤ ~4k токенов).
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
            "Produce a SHORT textual factor with 3–7 atomic observations for the day. "
            "Each observation MUST focus on ONE asset and include a brief causal reason "
            "for 1–3 day price impact. "
            "Additionally, assign smart topical tags per observation."
        )
        user = f"""Date: {date.date()}
Preference of the day: {self.preference or "нет"}

News (headline :: brief):
{news_blob}

Return STRICT JSON:
{{
  "observations": [
    {{
      "text": "Short atomic observation (what happened → why it matters → asset)",
      "asset": "MAIN SYMBOL in UPPERCASE or null if unsure",
      "symbols": ["LIST of possible symbols/aliases/tickers, can be empty"],
      "tags": ["LOWER_SNAKE_CASE topical tags, e.g. macro, onchain, etf, derivatives, orderbook, sentiment, tokenomics, stablecoins, ecosystem, protocol, narratives, cex, dex, liquidity"],
      "confidence": 0.0-1.0
    }}
  ]
}}
Rules:
- 3–7 observations; if a sentence mentions multiple assets, split into separate observations.
- DO NOT restrict symbols to a predefined list: if the asset is new/rare, still return it as-is (UPPERCASE). Contract/address can be noted as "CA:<address>" in symbols.
- Tags MUST be topical (not assets), in lower_snake_case. Create a new tag if necessary (keep it short and general).
- Keep it short (overall ≤ ~4k tokens).
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
            raw_tags = item.get("tags") or []
            conf = float(item.get("confidence", 1.0) or 1.0)

  
            if not asset:
                asset = guess_asset_simple(text)


            if asset and asset not in {"BTC", "ETH", "SOL", "BNB", "TON"}:
                self.logger.info("LLM detected possibly new/unknown symbol: %s (symbols=%s)", asset, symbols)


            tags = normalize_tags([str(t) for t in raw_tags], text=text, max_tags=3)
            self.logger.debug("Obs %d: asset=%s tags=%s conf=%.2f", i, asset, tags, conf)

            out.append(Observation(
                text=text,
                asset=asset,
                confidence=max(0.0, min(conf, 1.0)),
                tags=tags or ["news"]
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