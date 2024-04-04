import os
from logging import Logger

def get_scraping_client(job_id: int,
    logger: Logger):

    try:
         logger.info(f"{job_id}: Setting up Twitter client.")

         """Loads ScraperAPI and OpenAI API keys from environment variables and validates their presence."""
         scraperapi_key = os.getenv("SCRAPERAPI_KEY")
         openai_key = os.getenv("OPENAI_API_KEY")

         if not all([scraperapi_key, openai_key]):
             logger.error(f"{job_id}:Missing API credentials. Both ScraperAPI and OpenAI API keys are required.")
             raise ValueError("Missing required API credentials.")

         return scraperapi_key, openai_key

    except Exception as error:
        logger.error(f"{job_id}: Unable to setup the Twitter client. Error:\n{error}")
        raise error