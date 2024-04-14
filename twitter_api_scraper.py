import logging
from app.scraper import scrape
from app.sdk.models import KernelPlancksterSourceData, BaseJobState
from app.sdk.scraped_data_repository import ScrapedDataRepository
from app.setup import setup




def main(
    job_id: int,
    query: str,
    tracer_id: str,
    start_date: str,
    end_date: str,
    work_dir: str,
    kp_auth_token: str,
    kp_host: str,
    kp_port: int,
    kp_scheme: str,
    openai_api_key:str,
    scraper_api_key:str,
    log_level: str = "WARNING",

) -> None:

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=log_level)

    if not all([job_id, query, tracer_id, start_date, end_date, scraper_api_key]):
        logger.error(
            f"{job_id}: job_id, tracer_id, start_date, end_date, scraperapi api_key, openai api_key, and query must all be set."
        )
        raise ValueError(
            "job_id, tracer_id, start_date, end_date, api_key and query must all be set."
        )

    kernel_planckster, protocol, file_repository = setup(
        job_id=job_id,
        logger=logger,
        kp_auth_token=kp_auth_token,
        kp_host=kp_host,
        kp_port=kp_port,
        kp_scheme=kp_scheme,
    )

    scraped_data_repository = ScrapedDataRepository(
        protocol=protocol,
        kernel_planckster=kernel_planckster,
        file_repository=file_repository,
    )

   

    scrape(
        job_id=job_id,
        tracer_id=tracer_id,
        query=query,
        start_date=start_date,
        end_date=end_date,
        scraper_api_key=scraper_api_key,
        openai_api_key=openai_api_key,
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
        "--kp-auth-token",
        type=str,
        default="",
        help="The Kernel Planckster auth token",
    )

    parser.add_argument(
        "--kp-host",
        type=str,
        default="localhost",
        help="The Kernel Planckster host",
    )

    parser.add_argument(
        "--kp-port",
        type=int,
        default=8000,
        help="The Kernel Planckster port",
    )

    parser.add_argument(
        "--kp-scheme",
        type=str,
        default="http",
        help="The Kernel Planckster scheme",
    )

    parser.add_argument(
        "--scraper_api_key",
        type=str,
        default="No Default Value possible",
        help="Scrape API key",
    )

    parser.add_argument(
        "--openai_api_key",
        type=str,
        default="",
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
        kp_auth_token=args.kp_auth_token,
        kp_host=args.kp_host,
        kp_port=args.kp_port,
        kp_scheme=args.kp_scheme,
        scraper_api_key=args.scraper_api_key,
        openai_api_key=args.openai_api_key
    )
