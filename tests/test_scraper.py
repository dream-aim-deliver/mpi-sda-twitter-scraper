import uuid
import twitter_api_scraper



def test_main_scraping(
) -> None:
    
    # Test the main scraping function
    twitter_api_scraper.main(
        job_id=1,
        query="narendra modi",
        tracer_id=f"test-{uuid.uuid4()}",
        log_level="INFO",
    )