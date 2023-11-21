import requests
import pandas as pd
import json
import logging
from pydantic import BaseModel

from app.sdk.models import LFN, BaseJobState, DataSource, Protocol

from app.twitter_scraper_impl import TwitterScraperJob
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway  # type: ignore
from app.sdk.minio_gateway import MinIORepository
from app.sdk.models import LFN, BaseJob, BaseJobState, DataSource, Protocol


logger = logging.getLogger(__name__)

class TwitterScrapeRequestModel(BaseModel):
    query: str
    outfile: str
    api_key: str


class TwitterAPIScraper:
    def __init__(self, config: TwitterScrapeRequestModel ) -> None:
        self.query = config.query
        self.outfile = config.outfile
        self.api_key = config.api_key
        self.logger = logger

    def execute(self):
        try:
            payload = {
                'api_key': self.api_key,
                'query': 'wildfire',
                'num': '100',
                'date_range_start': '2023-08-20',
                'date_range_end': '2023-09-20'
            }
            response = requests.get('https://api.scraperapi.com/structured/twitter/search', params=payload)

            data = json.loads(response.text)

            output = json.dumps(data, indent=4)

        except Exception as e:
            self.logger.error("API expired or account reached its maximum request limit")

        self.logger.info(output)

        data = json.loads(response.text)

        tweets = data['organic_results']

        for tweet in tweets:
            self.logger.info(tweet['title'])
            self.logger.info(tweet['snippet'])
            self.logger.info(tweet['link'])

        twitter_data = []
        for tweet in tweets:
            twitter_data.append({
                'Title': tweet["title"],
                'Tweet': tweet["snippet"],
                'URL': tweet["link"]
            })

        df = pd.DataFrame(twitter_data)
        df.to_json(f"{self.outfile}.json", orient='index')
        print('Export Successful')