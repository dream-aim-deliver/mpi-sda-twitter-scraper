import subprocess
import time
from datetime import datetime as datetime
import os
from dotenv import load_dotenv
from twitter_api_scraper import TwitterAPIScraper, TwitterScrapeRequestModel
from fastapi import FastAPI
from pydantic import BaseModel
import logging
import sys


load_dotenv()
logging.basicConfig(level=logging.INFO
                    , format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    , datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")

app = FastAPI()

class TwitterScrapeRequest(BaseModel):
    query: str = "wildfire"

@app.post("/twitter/scrape")
def scrape_twitter(request: TwitterScrapeRequest):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    outfile = f"data_twitter_{request.query.replace(' ', '-')}_{timestamp}.csv"

    logger.info(f"{outfile}: {datetime.now()} - Starting scraping {outfile}")
    start_time = time.time()
    scrape_request: TwitterScrapeRequestModel = TwitterScrapeRequestModel(
        query=request.query,
        outfile=outfile,
        api_key=API_KEY
    )
    scraper = TwitterAPIScraper(scrape_request)
    # This should be a background task
    scraper.execute()

    logger.info(f"{outfile}: {datetime.now()} - Scraping completed in {time.time() - start_time} seconds. {outfile}")

    return {"data": "Scraping completed ${outfile}"}

def server() -> None:
    cmd = ["uvicorn", "server:app", "--reload", "--host", f"{HOST}", "--port", f"{PORT}"]
    subprocess.call(cmd)

if __name__ == "__main__":
    server()