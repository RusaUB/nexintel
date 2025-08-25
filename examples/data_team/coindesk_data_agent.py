from __future__ import annotations
import logging
from datetime import datetime

from utils.config_loader import load_config, load_env_profile   
from utils.logging_setup import setup_logging_from_yaml

from data.sources.news import CoinDeskSource
from agents.news_agent import NewsDataAgent
from factors.tag_split import split_factor_by_tags_configured

def main():
    env = load_env_profile()  

    cfg = load_config()

    setup_logging_from_yaml(cfg)
    log = logging.getLogger("NexIntel.Main")
    log.info("Starting NexIntel in env=%s", env)

    coindesk_cfg = (cfg.get("data_sources") or {}).get("coindesk") or {}
    secrets_cfg  = cfg.get("secrets", {})

    source = CoinDeskSource.from_config(coindesk_cfg, secrets_cfg)
    source.connect()
    raw = source.fetch(start=None, end=None)
    events = source.normalize(raw)
    source.close()

    agent_cfg = (cfg.get("agents") or {}).get("news_data_agent") or {}
    agent = NewsDataAgent.from_config(agent_cfg, secrets_cfg=secrets_cfg)
    factor = agent.run(date=datetime.now(), events=events)


    tag_factors = split_factor_by_tags_configured(factor, cfg)
    log.info("Produced %d tag factors from %s", len(tag_factors), factor.agent_name)

    for tf in tag_factors:
        print(tf.agent_name, "obs:", len(tf.observations))

if __name__ == "__main__":
    main()