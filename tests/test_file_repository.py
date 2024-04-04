import os
import tempfile
import uuid

from app.sdk.file_repository import FileRepository
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.models import KernelPlancksterSourceData, ProtocolEnum


def test_file_name_to_pfn_and_back(
    file_repository: FileRepository,
) -> None:

    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    file_name = tmp_file.name

    pfn = file_repository.file_name_to_pfn(file_name)

    assert file_repository.pfn_to_file_name(pfn) == file_name

    # Clean up
    os.remove(file_name)


def test_save_file_locally(
    file_repository: FileRepository,
    test_file_path: str,
) ->  None:

    tmp_file = test_file_path

    relative_path = "test_path"

    source_data = KernelPlancksterSourceData(
        name="test_name",
        protocol=ProtocolEnum.LOCAL,
        relative_path=relative_path,
    )

    type = "test_type"

    pfn = file_repository.save_file_locally(tmp_file, source_data, type)
    
    new_file = file_repository.pfn_to_file_name(pfn)

    # Assert the new file saved is the same as the original file
    with open(tmp_file, "rb") as f:
        original = f.read()

    with open(new_file, "rb") as f:
        saved = f.read()
    
    assert original == saved

    # Clean up
    os.remove(new_file)
    os.remove(tmp_file)


def test_public_upload(
        kernel_planckster_gateway: KernelPlancksterGateway,
        test_file_path: str,
        file_repository: FileRepository,
) -> None:

    kernel_planckster = kernel_planckster_gateway

    tmp_file = test_file_path

    test_media_sd = KernelPlancksterSourceData(
        name="test_name",
        protocol=ProtocolEnum.S3,
        relative_path=f"test_path-{uuid.uuid4()}.ext"
    )

    signed_url = kernel_planckster.generate_signed_url(test_media_sd)
    

    file_repository.public_upload(
        signed_url=signed_url,
        file_path=tmp_file,
    )