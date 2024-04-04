import logging
from logging import Logger
import json
import pandas as pd
import requests
from pydantic import BaseModel, Literal
from typing import List
from instructor import patch, OpenAI
from app.sdk.models import KernelPlancksterSourceData, BaseJobState, JobOutput
from app.sdk.scraped_data_repository import ScrapedDataRepository

logger = logging.getLogger(__name__)


class MessageData(BaseModel):
    city: str
    country: str
    year: int
    month: Literal['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    day: Literal['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
    disaster_type: str


class FilterData(BaseModel):
    relevant: bool


class TwitterScrapeRequestModel(BaseModel):
    query: str
    outfile: str
    api_key: str


class TwitterAPIScraper:
    def __init__(self, config: TwitterScrapeRequestModel) -> None:
        self.query = config.query
        self.outfile = config.outfile
        self.api_key = config.api_key
        self.logger = logger

    async def scrape(
        self,
        job_id: int,
        tracer_id: str,
        scraped_data_repository: ScrapedDataRepository,
        log_level: Logger
    ) -> JobOutput:
        try:
            logger = logging.getLogger(__name__)
            logging.basicConfig(level=log_level)

            job_state = BaseJobState.CREATED
            current_data: KernelPlancksterSourceData | None = None
            last_successful_data: KernelPlancksterSourceData | None = None

            protocol = scraped_data_repository.protocol
            output_data_list: List[KernelPlancksterSourceData] = []
            client = patch(OpenAI())
            logger.info(f"{job_id}: Starting Job")
            job_state = BaseJobState.RUNNING
            
            payload = {
                'api_key': self.api_key,
                'query': self.query,
                'num': '100',
                'date_range_start': '2023-08-20',
                'date_range_end': '2023-09-20'
            }
            response = requests.get('https://api.scraperapi.com/structured/twitter/search', params=payload)
            data = json.loads(response.text)

            output = json.dumps(data, indent=4)
            self.logger.info(output)

            tweets = data.get('organic_results', [])

            output_data_list: List[KernelPlancksterSourceData] = []
            job_state = BaseJobState.CREATED
            current_data: KernelPlancksterSourceData | None = None
            last_successful_data: KernelPlancksterSourceData | None = None

            for tweet in tweets:
                self.logger.info(tweet.get('title', ''))
                self.logger.info(tweet.get('snippet', ''))
                self.logger.info(tweet.get('link', ''))

                filter_data = client.chat.completions.create(
                    model="gpt-4",
                    response_model=FilterData,
                    messages=[
                        {
                            "role": "user",
                            "content": f"Examine this tweet: {tweet['snippet']}. Is this tweet describing {filter_keyword}? "
                        },
                    ]
                )

                if filter_data.relevant:
                    aug_data = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        response_model=MessageData,
                        messages=[
                            {
                                "role": "user",
                                "content": f"Extract: {tweet['snippet']}"
                            },
                        ]
                    )

                    city = aug_data.city
                    country = aug_data.country
                    year = aug_data.year
                    month = aug_data.month
                    day = aug_data.day
                    disaster_type = aug_data.disaster_type

                    backtrace = tweet['snippet']  # maybe replace with ID system

                    location_obj = [[city, country], [backtrace]]
                    date_obj = [[day, month, year], [backtrace]]
                    event = [[disaster_type], [backtrace]]

                    ##################################################################
                    # publish these to kafka topics
                    ##################################################################

                    data_name = f"{year}-{month}-{day}"
                    protocol = "twitter"
                    relative_path = f"twitter/{tracer_id}/{job_id}/{data_name}.json"

                    media_data = KernelPlancksterSourceData(
                        name=data_name,
                        protocol=protocol,
                        relative_path=relative_path,
                    )
                    current_data = media_data
                    output_data_list.append(media_data)

                    scraped_data_repository.register_scraped_data(
                        job_id=job_id,
                        source_data=media_data,
                        data=data_name,
                    )

            job_state = BaseJobState.FINISHED

            return JobOutput(
                job_state=job_state,
                tracer_id=tracer_id,
                source_data_list=output_data_list,
            )

        except Exception as e:
            self.logger.error("API expired or account reached its maximum request limit")
