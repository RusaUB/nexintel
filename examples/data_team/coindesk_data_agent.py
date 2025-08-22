import os
from dotenv import load_dotenv
from data.sources.news import CoinDeskSource

from agents.news_agent import NewsDataAgent
import datetime

load_dotenv()
source = CoinDeskSource(api_key=os.getenv("COINDESK_API_KEY"), base_url="https://data-api.coindesk.com/news/v1/article/list")
source.connect()
raw_data = source.fetch(start=None,end=None)
events = source.normalize(raw=raw_data)
source.close()

agent = NewsDataAgent()
factor = agent.run(date=datetime.datetime.now(),events=events)

print(factor)