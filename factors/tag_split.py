from __future__ import annotations
import logging
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any

from factors.schema import TextualFactor, Observation

logger = logging.getLogger("NexIntel.Factors.TagSplit")

# Default fallback priority (used if config priority is missing)
DEFAULT_TAG_PRIORITY = [
    "etf", "onchain", "derivatives", "orderbook", "tokenomics", "stablecoins",
    "protocol", "ecosystem", "liquidity", "narratives", "macro", "regulation",
    "cex", "dex", "sentiment", "news",
]


def _norm_tag(t: str) -> str:
    """Lowercase + snake_case normalization for tags."""
    t = (t or "").strip().lower()
    t = re.sub(r"[^a-z0-9]+", "_", t)
    return re.sub(r"_+", "_", t).strip("_")

def _rough_token_len(*texts: str) -> int:
    """Cheap token length proxy."""
    words = sum(len((t or "").split()) for t in texts)
    return int(words * 1.3)

def _obs_key(obs: Observation) -> str:
    """Global dedup key: (ASSET|TEXT)."""
    text_norm = re.sub(r"\s+", " ", (obs.text or "")).strip().lower()
    return f"{(obs.asset or 'NA').upper()}|{text_norm}"


@dataclass
class TagSplitConfig:
    """Runtime configuration for tag-based factor splitting (loaded from YAML)."""
    enabled: bool = True
    priority: List[str] = field(default_factory=lambda: DEFAULT_TAG_PRIORITY.copy())
    max_obs_per_factor: int = 7
    max_tokens_factor: int = 4000
    fallback_tag: str = "misc"

    @classmethod
    def from_yaml(cls, cfg: Dict[str, Any]) -> "TagSplitConfig":
        """
        Reads config from:
          cfg["agents"]["news_data_agent"]["split_by_tags"]
        Expected keys:
          enabled: bool
          priority: [list of tags]
          per_factor_limits: { max_obs: int, max_tokens_factor: int }
          fallback_tag: str
        """
        sbt = ((cfg.get("agents") or {})
                  .get("news_data_agent") or {}
              ).get("split_by_tags") or {}

        pfl = sbt.get("per_factor_limits") or {}
        return cls(
            enabled = bool(sbt.get("enabled", True)),
            priority = list(sbt.get("priority", DEFAULT_TAG_PRIORITY)),
            max_obs_per_factor = int(pfl.get("max_obs", 7)),
            max_tokens_factor = int(pfl.get("max_tokens_factor", 4000)),
            fallback_tag = sbt.get("fallback_tag", "misc"),
        )


class TagSplitter:
    """Splits a TextualFactor into multiple single-tag factors with no duplicate observations."""

    def __init__(self, config: TagSplitConfig):
        self.cfg = config
        # Build a priority index for O(1) comparisons
        self._prio_index: Dict[str, int] = {t: i for i, t in enumerate(self.cfg.priority)}

    def _choose_primary_tag(self, tags: List[str]) -> Optional[str]:
        """Pick the primary tag by config priority; unknown tags go last."""
        if not tags:
            return None
        tags_n = [_norm_tag(t) for t in tags if t]
        # Sort by configured priority; items not in the priority list go to the end
        tags_n.sort(key=lambda t: self._prio_index.get(t, 10**9))
        return tags_n[0] if tags_n else None

    def split(self, factor: TextualFactor) -> List[TextualFactor]:
        """
        Divide one TextualFactor into factors by primary tag:
          - each Observation goes to EXACTLY ONE tag-bucket;
          - no duplicates across buckets;
          - apply per-factor limits (max_obs_per_factor, max_tokens_factor).
        """
        if not self.cfg.enabled:
            logger.info("TagSplit disabled in config; returning original factor")
            return [factor]

        logger.info("TagSplit: factor by %s, obs=%d", factor.agent_name, len(factor.observations))

        # Assign each observation a primary tag (fallback if missing)
        assignments: List[Tuple[Observation, str]] = []
        for obs in factor.observations:
            primary = self._choose_primary_tag(obs.tags or [])
            if not primary:
                primary = self.cfg.fallback_tag
            assignments.append((obs, primary))

        # Group by tag with global dedup + per-bucket limits
        out_factors: List[TextualFactor] = []
        by_tag: Dict[str, List[Observation]] = {}
        seen_keys: set[str] = set()

        for obs, tag in assignments:
            key = _obs_key(obs)
            if key in seen_keys:
                continue  # already placed in another tag-bucket

            bucket = by_tag.setdefault(tag, [])
            # Estimate if the next observation fits the limits
            cand_tokens = _rough_token_len(obs.text)
            current_tokens = sum(_rough_token_len(o.text) for o in bucket)

            if (len(bucket) < self.cfg.max_obs_per_factor and
                current_tokens + cand_tokens <= self.cfg.max_tokens_factor):
                bucket.append(obs)
                seen_keys.add(key)

        #  Build a single-tag TextualFactor per tag
        for tag, obs_list in by_tag.items():
            if not obs_list:
                continue
            length_tokens = sum(_rough_token_len(o.text) for o in obs_list)
            tf = TextualFactor(
                date=factor.date,
                agent_name=f"{factor.agent_name}#{tag}",  # stable id useful for Predict history
                observations=obs_list,
                length_tokens=length_tokens,
                preference=tag,                 # handy for logging/debug
                raw_sources=factor.raw_sources  # keep provenance
            )
            logger.info("TagSplit: built %s obs=%d tokens≈%d", tf.agent_name, len(obs_list), length_tokens)
            out_factors.append(tf)

        #  Stable order
        out_factors.sort(key=lambda x: x.agent_name)
        return out_factors


def split_factor_by_tags_configured(
    factor: TextualFactor,
    full_cfg: Dict[str, Any]
) -> List[TextualFactor]:
    """
    One-call splitter driven by the YAML:
      factors = split_factor_by_tags_configured(factor, cfg)
    """
    cfg = TagSplitConfig.from_yaml(full_cfg)
    splitter = TagSplitter(cfg)
    return splitter.split(factor)

def choose_primary_tag(tags: List[str], tag_priority: Optional[List[str]] = None) -> Optional[str]:
    """
    Legacy helper kept for backward compatibility.
    Prefer using TagSplitter._choose_primary_tag driven by YAML.
    """
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
    Legacy functional splitter (manual params).
    Prefer using split_factor_by_tags_configured(factor, cfg).
    """
    cfg = TagSplitConfig(
        enabled=True,
        priority=tag_priority or DEFAULT_TAG_PRIORITY,
        max_obs_per_factor=max_obs_per_factor,
        max_tokens_factor=max_tokens_factor,
        fallback_tag=fallback_tag,
    )
    return TagSplitter(cfg).split(factor)