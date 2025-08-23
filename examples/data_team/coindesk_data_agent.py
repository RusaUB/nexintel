import os
from dotenv import load_dotenv
from data.sources.news import CoinDeskSource
import yaml
from agents.news_agent import NewsDataAgent
from factors.tag_split import split_factor_by_tags
import datetime

with open("config/default.yaml", "r") as f:
    cfg = yaml.safe_load(f)

coindesk_cfg = cfg["data_sources"]["coindesk"]
agent_cfg = cfg["agents"]["news_data_agent"]
secrets_cfg  = cfg.get("secrets", {})

load_dotenv()
source = CoinDeskSource.from_config(coindesk_cfg, secrets_cfg)
source.connect()
raw_data = source.fetch(start=None,end=None)
events = source.normalize(raw=raw_data)
source.close()

agent = NewsDataAgent.from_config(agent_cfg, secrets_cfg=secrets_cfg)
factor = agent.run(date=datetime.datetime.now(),events=events)


tag_factors = split_factor_by_tags(factor,cfg)

print(tag_factors)