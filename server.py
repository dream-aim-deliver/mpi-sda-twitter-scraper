import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.sdk.job_manager import BaseJobManager
from app.sdk.job_router import JobManagerFastAPIRouter
from twitter_api_scraper import scrape
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

HOST = os.getenv("HOST", "localhost")
PORT = int(os.getenv("PORT", "8000"))
MODE = os.getenv("MODE", "production")

app = FastAPI()
app.job_manager = BaseJobManager()  # type: ignore

# Define a request model for the API
class ScrapeRequest(BaseModel):
    query: str
    start_date: str
    end_date: str

@app.post("/scrape/")
async def perform_scrape(request: ScrapeRequest):
    try:
        # Call the scrape function
        scrape(
            job_id="1",  # dynamic handling
            query=request.query,
            tracer_id="1",  # dynamic handling
            start_date=request.start_date,
            end_date=request.end_date,
            work_dir="./.tmp",
            kp_auth_token=os.getenv("KP_AUTH_TOKEN", ""),
            kp_host=os.getenv("KP_HOST", "localhost"),
            kp_port=int(os.getenv("KP_PORT", "8000")),
            kp_scheme=os.getenv("KP_SCHEME", "http"),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            scraper_api_key=os.getenv("SCRAPER_API_KEY", "")
        )
        return {"message": "Scrape initiated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Setup the job manager router
job_manager_router = JobManagerFastAPIRouter(app, scrape)
app.include_router(job_manager_router, prefix="/jobs")

if __name__ == "__main__":
    import uvicorn
    print(f"Starting server on {HOST}:{PORT}")
    uvicorn.run("server:app", host=HOST, port=PORT, reload=True)
