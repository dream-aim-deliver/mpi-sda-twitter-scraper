from logging import Logger
import logging
import requests
import pandas as pd
import json
import time
import sys
from app.sdk.models import KernelPlancksterSourceData, BaseJobState, JobOutput
from app.sdk.scraped_data_repository import ScrapedDataRepository
import os 
from pydantic import BaseModel
from typing import Literal
import instructor
from instructor import Instructor
from openai import OpenAI
from geopy.geocoders import Nominatim
import shutil 
import uuid
import re

class messageData(BaseModel):
    city: str
    country: str
    year: int
    month: Literal['January', 'Febuary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    day: Literal['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
    disaster_type: Literal['Wildfire', 'Climate' 'Other']

#Potential alternate prompting
# class messageDataAlternate(BaseModel):
#     city: str
#     country: str
#     year: int
#     month: Literal['January', 'Febuary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December', 'Unsure']
#     day: Literal['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', 'Unsure']
#     disaster_type: Literal['Wildfire', 'Other']

class filterData(BaseModel):
    relevant: bool

class TwitterScrapeRequestModel(BaseModel):
    query: str
    outfile: str
    api_key: str
    


def scrape(
    job_id: int,
    tracer_id:str,
    query: str,
    start_date: str,
    end_date: str,
    scraped_data_repository: ScrapedDataRepository,
    work_dir: str,
    log_level: Logger,
    scraper_api_key: str,
    openai_api_key: str
) -> JobOutput:
    try:
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=log_level)

        job_state = BaseJobState.CREATED
        current_data: KernelPlancksterSourceData | None = None
        last_successful_data: KernelPlancksterSourceData | None = None

        protocol = scraped_data_repository.protocol

        output_data_list: list[KernelPlancksterSourceData] = []

        filter = query
        # Enables `response_model`
        client = instructor.from_openai(OpenAI(api_key=openai_api_key))

        logger.info(f"{job_id}: Starting Job")
        job_state = BaseJobState.RUNNING
        results = []
        augmented_results = []
        page = 1
        tweet_count = 0
        while True:
            payload = {
                'api_key': scraper_api_key,
                'query': query,
                'date_range_start': start_date,
                'date_range_end': end_date,
                'page': page,
                'format': 'json'
            }
            try:
                response = requests.get('https://api.scraperapi.com/structured/twitter/search', params=payload)
                response.raise_for_status()
                data = response.json()
                if 'organic_results' in data:
                    new_tweets = data['organic_results']
                    results.extend(new_tweets)

                    augmented_tweet = None
                    for tweet in new_tweets:
                        if tweet != None: 
                            augmented_tweet = augment_tweet(client, tweet, filter)
                        if augmented_tweet != None:
                            augmented_results.append(augmented_tweet)
                    
                    tweet_count += len(new_tweets)
                    logger.info(f"{job_id}: Fetched {tweet_count} tweets so far...")
                    
                    current_data = KernelPlancksterSourceData(
                        name=f"tweet_{page}",
                        protocol=protocol,
                        relative_path=f"twitter/{tracer_id}/{job_id}/scraped/tweet_{page}.json",
                    )
                    output_data_list.append(current_data)
                    lfp = f"{work_dir}/twitter/tweet_{page}.json"

                    save_tweets(new_tweets, lfp)
                    try:
                        scraped_data_repository.register_scraped_json(current_data, job_id, lfp )
                    except Exception as e:
                        logger.info("could not register file")
                    last_successful_data = current_data
                else:
                    logger.error(f"{job_id}: Error: {response.status_code} - {response.text}")
                    logger.info("No more tweets found for this query. Scraping completed.")
                
                    save_tweets(results, f"{work_dir}/twitter/tweet_all.json")
                    
                    final_data = KernelPlancksterSourceData(
                        name=f"tweet_all",
                        protocol=protocol,
                        relative_path=f"twitter/{tracer_id}/{job_id}/scraped/tweet_all.json",
                    )
                    try:
                        scraped_data_repository.register_scraped_json(final_data, job_id, f"{work_dir}/twitter/tweet_all.json" )
                    except Exception as e:
                        logger.info("could not register file")
                    # write augmented data to file: --> title, content, extracted_location, lattitude, longitude, month, day, year, disaster_type

                    df = pd.DataFrame(augmented_results, columns=["Title", "Tweet", "Extracted_Location", "Resolved_Latitude", "Resolved_Longitude", "Month", "Day", "Year", "Disaster_Type"])
                    df.to_json(f"{work_dir}/twitter/augmented_twitter_scrape.json", orient='index', indent=4)

                    final_augmented_data = KernelPlancksterSourceData(
                        name=f"tweet_all_augmented",
                        protocol=protocol,
                        relative_path=f"twitter/{tracer_id}/{job_id}/augmented/data.json",
                    )
                    try:
                        scraped_data_repository.register_scraped_json(final_augmented_data, job_id, f"{work_dir}/twitter/augmented_twitter_scrape.json" )
                    except Exception as e:
                        logger.info("could not register file")
                    break
            except requests.exceptions.HTTPError as e:
                job_state = BaseJobState.FAILED
                logger.error(f"{job_id}: HTTP Error: {e}")
                continue
            except requests.exceptions.JSONDecodeError as e:
                job_state = BaseJobState.FAILED
                logger.error(f"{job_id}: JSON Decode Error: {e}")
                logger.info("Retrying request after a short delay...")
                time.sleep(5)
                continue
            except Exception as e:
                job_state = BaseJobState.FAILED
                logger.error(f"{job_id}: Unable to scrape data. Error:\n{e}\nJob with tracer_id {job_id} failed.\nLast successful data: {last_successful_data}\nCurrent data: \"{current_data}\", job_state: \"{job_state}\"")
                continue

            page += 1
            time.sleep(1)

    

        job_state = BaseJobState.FINISHED
        logger.info(f"{job_id}: Job finished")
        try:
            shutil.rmtree(work_dir)
        except Exception as e:
            logger.log("Could not delete tmp directory, exiting")
        return JobOutput(
            job_state=job_state,
            tracer_id=str(job_id),
            source_data_list=output_data_list,
        )

    except Exception as error:
        logger.error(f"{job_id}: Unable to scrape data. Job with tracer_id {job_id} failed. Error:\n{error}")
        job_state = BaseJobState.FAILED
        try:
            shutil.rmtree(work_dir)
        except Exception as e:
            logger.log("Could not delete tmp directory, exiting")
        return JobOutput(
            job_state=job_state,
            tracer_id=str(job_id),
            source_data_list=[],
        )

def extract_username(snippet):
    #search for username in extracted tweet
    match = re.search(r'@\w+', snippet)
    return match.group(0)[1:] if match else "unknown"



def save_tweets(tweets, work_dir):
    # Ensure the working directory exists
    os.makedirs(work_dir, exist_ok=True)
    
    # Saving them individually
    for i, tweet in enumerate(tweets):
        snippet = tweet['tweet']['snippet']
        username = extract_username(snippet)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex
        filename = f"{username}_{timestamp}_{unique_id}.json"
        file_path = os.path.join(work_dir, filename)
        
        # Save each tweet to a unique file
        with open(file_path, 'w+') as f:
            json.dump(tweet, f)

def load_tweets(file_path):
    out = None
    with open(file_path, 'r') as f:
        out = json.load(f)
    return out

def augment_tweet(client:Instructor , tweet: dict, filter: str):
    if len(tweet) > 5:
        # extract aspects of the tweet
            title = tweet["title"]
            content = tweet["snippet"]
            link = tweet["link"]
            
            formatted_tweet_str = "User " + title + " tweets: " + content.strip("...").strip(",").strip() + "."
            
            #relvancy filter with gpt-4 
            filter_data = client.chat.completions.create(
                model="gpt-4",
                response_model=filterData,
                messages=[
                    {
                    "role": "user", 
                    "content": f"Examine this tweet: {formatted_tweet_str}. Is this tweet describing {filter}? "
                    },
                ]
            )
            
            if filter_data.relevant == True:
                aug_data = None
                try:
                    #location extraction with gpt-3.5
                    aug_data = client.chat.completions.create(
                    model="gpt-4-turbo", 
                    response_model=messageData,
                    messages=[
                        {
                        "role": "user", 
                        "content": f"Extract: {formatted_tweet_str}"
                        },
                    ]
                    )
                except Exception as e:
                    Logger.info("Could not augment tweet, trying with alternate prompt")
                    #Potential alternate prompting
                    
                    # try:
                    #     #location extraction with gpt-3.5
                    #     aug_data = client.chat.completions.create(
                    #     model="gpt-4-turbo", 
                    #     response_model=messageDataAlternate,
                    #     messages=[
                    #         {
                    #         "role": "user", 
                    #         "content": f"Extract: {formatted_tweet_str}"
                    #         },
                    #     ]
                    #     )
                    # except Exception as e2:
                    return None
                city = aug_data.city
                country = aug_data.country
                extracted_location = city + "," + country 
                year = aug_data.year
                month = aug_data.month
                day = aug_data.day
                disaster_type = aug_data.disaster_type   
                
                # NLP-informed geolocation            
                try:
                    coordinates = get_lat_long(extracted_location)
                except Exception as e:
                    coordinates = None
                if coordinates:
                    lattitude = coordinates[0]
                    longitude = coordinates[1]
                else:
                    lattitude = "no latitude"
                    longitude = "no longitude"

                #TODO: format date
                
                return [title, content, extracted_location, lattitude, longitude, month, day, year, disaster_type]

# utility function for augmenting tweets with geolocation
def get_lat_long(location_name):
    geolocator = Nominatim(user_agent="location_to_lat_long")
    try:
        location = geolocator.geocode(location_name)
        if location:
            latitude = location.latitude
            longitude = location.longitude
            return latitude, longitude
        else:
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    
