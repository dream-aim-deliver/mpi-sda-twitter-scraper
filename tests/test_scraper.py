import uuid
import telegram_scraper



def test_main_scraping(
) -> None:
    
    # Test the main scraping function
    telegram_scraper.main(
        job_id=1,
        channel_name="sda_test",
        tracer_id=f"test-{uuid.uuid4()}",
        log_level="INFO",
    )