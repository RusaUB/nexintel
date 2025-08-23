from __future__ import annotations
import logging, re
from typing import List, Dict, Optional, Tuple

from factors.schema import TextualFactor, Observation

logger = logging.getLogger("NexIntel.Factors.TagSplit")

DEFAULT_TAG_PRIORITY = [
    "etf", "onchain", "derivatives", "orderbook", "tokenomics", "stablecoins",
    "protocol", "ecosystem", "liquidity", "narratives", "macro", "regulation",
    "cex", "dex", "sentiment", "news",
]

def _norm_tag(t: str) -> str:
    t = (t or "").strip().lower()
    t = re.sub(r"[^a-z0-9]+", "_", t)
    return re.sub(r"_+", "_", t).strip("_")

def _rough_token_len(*texts: str) -> int:
    words = sum(len((t or "").split()) for t in texts)
    return int(words * 1.3)

def _obs_key(obs: Observation) -> str:
    text_norm = re.sub(r"\s+", " ", (obs.text or "")).strip().lower()
    return f"{(obs.asset or 'NA').upper()}|{text_norm}"

def choose_primary_tag(tags: List[str],
                      tag_priority: Optional[List[str]] = None) -> Optional[str]:
    if not tags:
        return None
    tags_n = [_norm_tag(t) for t in tags if t]
    pr = tag_priority or DEFAULT_TAG_PRIORITY
    pr_index = {t: i for i, t in enumerate(pr)}
    tags_n.sort(key=lambda t: pr_index.get(t, 10**9))
    return tags_n[0] if tags_n else None

def split_factor_by_tags(
    factor: TextualFactor,
    tag_priority: Optional[List[str]] = None,
    max_obs_per_factor: int = 7,
    max_tokens_factor: int = 4000,
    fallback_tag: str = "misc"
) -> List[TextualFactor]:
    """
    Divides one TextualFactor into a set of factors by tags:
    - each observation → EXACTLY ONE factor (by primary tag);
    - no duplicate observations;
    - limits on the number of observations and tokens per factor are preserved.
    """
    logger.info("TagSplit: factor by %s, obs=%d", factor.agent_name, len(factor.observations))

    assignments: List[Tuple[Observation, str]] = []
    for obs in factor.observations:
        primary = choose_primary_tag(obs.tags or [], tag_priority=tag_priority)
        if not primary:
            primary = fallback_tag
        assignments.append((obs, primary))

    out_factors: List[TextualFactor] = []
    by_tag: Dict[str, List[Observation]] = {}
    seen_keys: set[str] = set() 

    for obs, tag in assignments:
        key = _obs_key(obs)
        if key in seen_keys:
            continue  
        bucket = by_tag.setdefault(tag, [])
        cand_tokens = _rough_token_len(obs.text)
        current_tokens = sum(_rough_token_len(o.text) for o in bucket)
        if len(bucket) < max_obs_per_factor and current_tokens + cand_tokens <= max_tokens_factor:
            bucket.append(obs)
            seen_keys.add(key)

    for tag, obs_list in by_tag.items():
        if not obs_list:
            continue
        length_tokens = sum(_rough_token_len(o.text) for o in obs_list)
        tf = TextualFactor(
            date=factor.date,
            agent_name=f"{factor.agent_name}#{tag}",  
            observations=obs_list,
            length_tokens=length_tokens,
            preference=tag,                
            raw_sources=factor.raw_sources  
        )
        logger.info("TagSplit: built %s obs=%d tokens≈%d", tf.agent_name, len(obs_list), length_tokens)
        out_factors.append(tf)

    out_factors.sort(key=lambda x: x.agent_name)
    return out_factors