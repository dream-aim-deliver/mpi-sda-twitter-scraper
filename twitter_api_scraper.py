import requests
import pandas as pd
import json
import logging
import pandas as pd  
from pydantic import BaseModel
from typing import Literal
from openai import OpenAI
import os
import instructor

logger = logging.getLogger(__name__)


class messageData(BaseModel):
    city: str
    country: str
    year: int
    month: Literal['January', 'Febuary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    day: Literal['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
    disaster_type: str
    
class filterData(BaseModel):
    relevant: bool

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
            filter = "forest wildfire"
            # Enables `response_model`
            client = instructor.patch(OpenAI())
                    
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
            
                        
            filter_data = client.chat.completions.create(
                model="gpt-4",
                response_model=filterData,
                messages=[
                    {
                    "role": "user", 
                    "content": f"Examine this tweet: {tweet['snippet']}. Is this tweet describing {filter}? "
                    },
                ]
            )
            
            if filter_data.relevant == True:
            
                aug_data = client.chat.completions.create(
                model="gpt-3.5-turbo", 
                response_model=messageData,
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
                
                backtrace = tweet['snippet'] #maybe replace with ID system
                
                location_obj = [[city, country], [backtrace]]
                date_obj = [[day, month, year], [backtrace]]
                event = [[disaster_type], [backtrace]]
         
                ##################################################################
                # publish these to kafka topics
                ##################################################################


        df = pd.DataFrame(twitter_data)
        df.to_json(f"{self.outfile}.json", orient='index')
        print('Export Successful')