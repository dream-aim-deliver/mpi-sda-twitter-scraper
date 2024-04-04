import os
import tempfile
from dotenv import load_dotenv
import pytest

from app.sdk.file_repository import FileRepository
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.models import ProtocolEnum 


load_dotenv()

@pytest.fixture
def kernel_planckster_config() -> dict[str, str]:
    return {
        "host": os.getenv("KERNEL_PLANCKSTER_HOST","localhost"),
        "port": os.getenv("KERNEL_PLANCKSTER_PORT","8000"),
        "auth_token": os.getenv("KERNEL_PLANCKSTER_AUTH_TOKEN","test123"),
        "scheme": os.getenv("KERNEL_PLANCKSTER_SCHEME","http"),
    }

@pytest.fixture
def kernel_planckster_gateway(kernel_planckster_config) -> KernelPlancksterGateway:
    kernel_planckster = KernelPlancksterGateway(
        host=kernel_planckster_config["host"],
        port=kernel_planckster_config["port"],
        auth_token=kernel_planckster_config["auth_token"],
        scheme=kernel_planckster_config["scheme"],
    )
    return kernel_planckster

@pytest.fixture
def file_repository_config() -> dict[str, str]:
    return {
        "protocol": os.getenv("STORAGE_PROTOCOL", "local"),
    }

@pytest.fixture
def file_repository(file_repository_config) -> FileRepository:
    file_repository = FileRepository(
        protocol=ProtocolEnum(file_repository_config["protocol"]),
    )
    return file_repository

@pytest.fixture(scope="function")
def test_file_path() -> str:
    test_content = b"test content: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."

    with tempfile.NamedTemporaryFile(delete=False) as file:
        file.write(test_content)
        file_name = file.name

    return file.name