import subprocess
import time
from datetime import datetime as datetime
import os
from dotenv import load_dotenv
from twitter_api_scraper import TwitterAPIScraper, TwitterScrapeRequestModel
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from app.sdk.models import LFN, BaseJobState, DataSource, Protocol
import logging
import sys

from app.twitter_scraper_impl import TwitterScraperJob, TwitterScraperJobManager
from twitter_api_scraper import execute

load_dotenv()

logging.basicConfig(level=logging.INFO
                    , format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    , datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO
                    , format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    , datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")

app = FastAPI()

app.job_manager = TwitterScraperJobManager() # type: ignore
data_dir = os.path.join(os.path.dirname(__file__), "data")

    
@app.get("/job")
def list_all_jobs() -> List[TwitterScraperJob]:
    job_manager: TwitterScraperJobManager = app.job_manager # type: ignore
    return job_manager.list_jobs()

@app.post("/job")
def create_job(tracer_id: str) -> TwitterScraperJob:
    job_manager: TwitterScraperJobManager = app.job_manager # type: ignore
    job: TwitterScraperJob = job_manager.create_job(tracer_id) # type: ignore
    return job

@app.get("/job/{job_id}")
def get_job(job_id: int) -> TwitterScraperJob:
    job_manager: TwitterScraperJobManager = app.job_manager # type: ignore
    job = job_manager.get_job(job_id)
    return job
#######################################################################################
@app.post("/job/{job_id}/start")
def start_job(job_id: int, background_tasks: BackgroundTasks):
    job_manager: TwitterScraperJobManager = app.job_manager # type: ignore
    job = job_manager.get_job(job_id)
    if API_KEY is None:
        job.state = BaseJobState.FAILED
        job.messages.append("Status: FAILED. API_ID and API_HASH must be set. ")
        raise HTTPException(status_code=500, detail="API_ID and API_HASH must be set.")
    background_tasks.add_task(execute, job=job, query="Wildfires", api_key=API_KEY)
########################################################################################
@app.get("/lfn")
def create_lfn() -> LFN:
    lfn = LFN(
        protocol=Protocol.LOCAL,
        tracer_id="test",
        job_id=20,
        source=DataSource.TWITTER,
        relative_path="test.csv",
    )
    return lfn    
# class TwitterScrapeRequest(BaseModel):
#     query: str = "wildfire"

# @app.post("/twitter/scrape")
# def scrape_twitter(request: TwitterScrapeRequest):
#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     outfile = f"data_twitter_{request.query.replace(' ', '-')}_{timestamp}.csv"

#     logger.info(f"{outfile}: {datetime.now()} - Starting scraping {outfile}")
#     start_time = time.time()
#     scrape_request: TwitterScrapeRequestModel = TwitterScrapeRequestModel(
#         query=request.query,
#         outfile=outfile,
#         api_key=API_KEY
#     )
#     scraper = TwitterAPIScraper(scrape_request)
#     # This should be a background task
#     scraper.execute()

#     logger.info(f"{outfile}: {datetime.now()} - Scraping completed in {time.time() - start_time} seconds. {outfile}")

#     return {"data": "Scraping completed ${outfile}"}

# def server() -> None:
#     cmd = ["uvicorn", "server:app", "--reload", "--host", f"{HOST}", "--port", f"{PORT}"]
#     subprocess.call(cmd)

# if __name__ == "__main__":
#     server()