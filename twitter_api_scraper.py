import logging
from app.scraper import scrape
from app.sdk.models import KernelPlancksterSourceData, BaseJobState
from app.sdk.scraped_data_repository import ScrapedDataRepository
from app.setup import setup

from app.setup_scraping_client import get_scraping_client



def main(
    job_id: int,
    query: str,
    tracer_id: str,
    start_date: str,
    end_date: str,
    api_key: str,
    work_dir:str,
    log_level: str = "WARNING",
) -> None:

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=log_level)

    if not all([job_id, query, tracer_id, start_date, end_date, api_key]):
        logger.error(f"{job_id}: job_id, tracer_id, start_date, end_date, scraperapi api_key, openai api_key, and query must all be set.")
        raise ValueError("job_id, tracer_id, start_date, end_date, api_key and query must all be set.")


    kernel_planckster, protocol, file_repository = setup(
        job_id=job_id,
        logger=logger,
    )

    scraped_data_repository = ScrapedDataRepository(
        protocol=protocol,
        kernel_planckster=kernel_planckster,
        file_repository=file_repository,
    )

    #TODO: redundent
    twitter_client = get_scraping_client(
        job_id=job_id,
        logger=logger,
    )


    
    scrape(
        job_id=job_id,
        tracer_id=tracer_id,
        query=query,
        start_date= start_date,
        end_date= end_date,
        api_key=api_key,
        scraped_data_repository=scraped_data_repository,
        work_dir=work_dir,
        log_level=log_level,
    )
    


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Scrape data from twitter.")

    parser.add_argument(
        "--job-id",
        type=str,
        default="1",
        help="The job id",
    )

    parser.add_argument(
        "--query",
        type=str,
        default="Wildfire",
        help="Search Query",
    )

    parser.add_argument(
        "--tracer-id",
        type=str,
        default="1",
        help="The tracer id",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        help="The log level to use when running the scraper. Possible values are DEBUG, INFO, WARNING, ERROR, CRITICAL. Set to WARNING by default.",
    )

    parser.add_argument(
        "--start_date",
        type=str,
        default="2024-03-10",
        help="The start date",
    )

    parser.add_argument(
        "--end_date",
        type=str,
        default="2024-03-15",
        help="The end date",
    )

    parser.add_argument(
        "--work_dir",
        type=str,
        default="./.tmp",
        help="work dir",
    )

    parser.add_argument(
        "--api_key",
        type=str,
        default="No Default Value possible",
        help="Scrape API key",
    )

    args = parser.parse_args()


    main(
        job_id=args.job_id,
        query=args.query,
        tracer_id=args.tracer_id,
        log_level=args.log_level,
        start_date=args.start_date,
        end_date=args.end_date,
        work_dir=args.work_dir,
        api_key=args.api_key,
    )

