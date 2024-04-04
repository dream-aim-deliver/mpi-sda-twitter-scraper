
from logging import Logger
import os
from typing import Tuple

from dotenv import load_dotenv
from app.sdk.file_repository import FileRepository
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.models import ProtocolEnum


def _setup_kernel_planckster(
    job_id: int,
    logger: Logger,
) -> KernelPlancksterGateway:

    try:

        logger.info(f"{job_id}: Setting up Kernel Planckster Gateway.")
        # Check environment variables for the Kernel Planckster Gateway
        kernel_planckster_host = os.getenv("KERNEL_PLANCKSTER_HOST")
        kernel_planckster_port = os.getenv("KERNEL_PLANCKSTER_PORT")
        kernel_planckster_auth_token = os.getenv("KERNEL_PLANCKSTER_AUTH_TOKEN")
        kernel_planckster_scheme = os.getenv("KERNEL_PLANCKSTER_SCHEME")

        if not all([kernel_planckster_host, kernel_planckster_port, kernel_planckster_auth_token, kernel_planckster_scheme]):
            logger.error(f"{job_id}: KERNEL_PLANCKSTER_HOST, KERNEL_PLANCKSTER_PORT, KERNEL_PLANCKSTER_AUTH_TOKEN and KERNEL_PLANCKSTER_SCHEME must be set.")
            raise ValueError("KERNEL_PLANCKSTER_HOST, KERNEL_PLANCKSTER_PORT, KERNEL_PLANCKSTER_AUTH_TOKEN and KERNEL_PLANCKSTER_SCHEME must be set.")

        # Setup the Kernel Planckster Gateway
        kernel_planckster = KernelPlancksterGateway(
            host=kernel_planckster_host,
            port=kernel_planckster_port,
            auth_token=kernel_planckster_auth_token,
            scheme=kernel_planckster_scheme,
        )
        kernel_planckster.ping()
        logger.info(f"{job_id}: Kernel Planckster Gateway setup successfully.")

        return kernel_planckster

    except Exception as error:
        logger.error(f"{job_id}: Unable to setup the Kernel Planckster Gateway. Error:\n{error}")
        raise error


def _setup_file_repository(
    job_id: int,
    storage_protocol: ProtocolEnum,
    logger: Logger,
) -> FileRepository:
        
    try:
        logger.info(f"{job_id}: Setting up the File Repository.")

        if not storage_protocol:
            logger.error(f"{job_id}: STORAGE_PROTOCOL must be set.")
            raise ValueError("STORAGE_PROTOCOL must be set.")

        file_repository = FileRepository(
            protocol=storage_protocol,
        )

        logger.info(f"{job_id}: File Repository setup successfully.")

        return file_repository
    
    except Exception as error:
        logger.error(f"{job_id}: Unable to setup the File Repository. Error:\n{error}")
        raise error



def setup(
    job_id: int,
    logger: Logger,
) -> Tuple[KernelPlancksterGateway, ProtocolEnum, FileRepository]:
    """
    Setup the Kernel Planckster Gateway, the storage protocol and the file repository.

    NOTE: needs and '.env' file within context.
    """

    try:

        load_dotenv(
            dotenv_path=".env",
        ) 

        kernel_planckster = _setup_kernel_planckster(job_id, logger)


        logger.info(f"{job_id}: Checking storage protocol.")
        protocol = ProtocolEnum(os.getenv("STORAGE_PROTOCOL", ProtocolEnum.S3.value))

        if protocol not in [ProtocolEnum.S3, ProtocolEnum.LOCAL]:
            logger.error(f"{job_id}: STORAGE_PROTOCOL must be either 's3' or 'local'.")
            raise ValueError("STORAGE_PROTOCOL must be either 's3' or 'local'.")

        logger.info(f"{job_id}: Storage protocol: {protocol}")


        file_repository = _setup_file_repository(job_id, protocol, logger)


        return kernel_planckster, protocol, file_repository

    except Exception as error:
        logger.error(f"{job_id}: Unable to setup. Error:\n{error}")
        raise error