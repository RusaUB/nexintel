import os
from dotenv import load_dotenv
from data.sources.news import CoinDeskSource

from agents.news_agent import NewsDataAgent
from factors.tag_split import split_factor_by_tags
import datetime

load_dotenv()
source = CoinDeskSource(api_key=os.getenv("COINDESK_API_KEY"), base_url="https://data-api.coindesk.com/news/v1/article/list")
source.connect()
raw_data = source.fetch(start=None,end=None)
events = source.normalize(raw=raw_data)
source.close()

agent = NewsDataAgent()
factor = agent.run(date=datetime.datetime.now(),events=events)


tag_factors = split_factor_by_tags(
    factor,                      
    tag_priority=None,           
    max_obs_per_factor=7,        
    max_tokens_factor=4000,
    fallback_tag="misc",
)

print(tag_factors)