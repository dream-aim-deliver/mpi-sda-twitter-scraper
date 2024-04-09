from logging import Logger
import logging
import requests
import pandas as pd
import json
import time
import sys
from app.sdk.models import KernelPlancksterSourceData, BaseJobState, JobOutput
from app.sdk.scraped_data_repository import ScrapedDataRepository

async def scrape(
    job_id: int,
    query: str,
    start_date: str,
    end_date: str,
    api_key: str,
    scraped_data_repository: ScrapedDataRepository,
    log_level: Logger,
) -> JobOutput:
    try:
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=log_level)

        job_state = BaseJobState.CREATED
        current_data: KernelPlancksterSourceData | None = None
        last_successful_data: KernelPlancksterSourceData | None = None

        protocol = scraped_data_repository.protocol

        output_data_list: list[KernelPlancksterSourceData] = []

        logger.info(f"{job_id}: Starting Job")
        job_state = BaseJobState.RUNNING

        results = []
        page = 1
        tweet_count = 0
        while True:
            payload = {
                'api_key': api_key,
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
                    tweet_count += len(new_tweets)
                    logger.info(f"{job_id}: Fetched {tweet_count} tweets so far...")
                    current_data = KernelPlancksterSourceData(
                        name=f"tweet_{tweet_count}",
                        protocol=protocol,
                        relative_path=f"twitter/{job_id}/tweets.json",
                    )
                    output_data_list.append(current_data)
                    save_tweets(results, f"twitter/{job_id}/tweets.json")
                    last_successful_data = current_data
                else:
                    logger.error(f"{job_id}: Error: {response.status_code} - {response.text}")
                    logger.info("No more tweets found for this query. Scraping completed.")
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

        return JobOutput(
            job_state=job_state,
            tracer_id=str(job_id),
            source_data_list=output_data_list,
        )

    except Exception as error:
        logger.error(f"{job_id}: Unable to scrape data. Job with tracer_id {job_id} failed. Error:\n{error}")
        job_state = BaseJobState.FAILED
        return JobOutput(
            job_state=job_state,
            tracer_id=str(job_id),
            source_data_list=[],
        )

def save_tweets(tweets, file_path):
    with open(file_path, 'w') as f:
        tweet_data = [{"tweet": tweet, "tweet_number": i + 1} for i, tweet in enumerate(tweets)]
        json.dump(tweet_data, f)