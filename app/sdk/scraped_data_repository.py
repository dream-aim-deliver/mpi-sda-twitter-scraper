import logging
from app.sdk.file_repository import FileRepository
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.models import KernelPlancksterSourceData, ProtocolEnum



class ScrapedDataRepository:
    def __init__(
            self,
            protocol: ProtocolEnum,
            kernel_planckster: KernelPlancksterGateway,
            file_repository: FileRepository,
    ) -> None:
        self.protocol = protocol
        self.kernel_planckster = kernel_planckster
        self.file_repository = file_repository
        self._logger = logging.getLogger(__name__)

    @property
    def log_level(self) -> str:
        return self._log_level

    @property
    def logger(self) -> logging.Logger:

        return self._logger


    def register_scraped_photo(self, source_data: KernelPlancksterSourceData, job_id: int, local_file_name: str) -> KernelPlancksterSourceData:

        match self.protocol:

            case ProtocolEnum.S3:

                signed_url = self.kernel_planckster.generate_signed_url(source_data=source_data) 
                
                self.logger.info(f"{job_id}: Uploading photo to object store")

                self.file_repository.public_upload(signed_url, local_file_name)
                
                self.logger.info(
                f"{job_id}: Uploaded photo to {signed_url}"
                )

                self.kernel_planckster.register_new_source_data(source_data=source_data)


            case ProtocolEnum.LOCAL:
                # If local, then we don't use kernel planckster at all
                # NOTE: local is deprecated, use this only for quick tests
                self.file_repository.save_file_locally(
                file_to_save=local_file_name,
                source_data=source_data,
                file_type="photo",
                )                                    

        return source_data


    def register_scraped_video_or_document(self, source_data: KernelPlancksterSourceData, job_id: int, local_file_name: str) -> KernelPlancksterSourceData:

        match self.protocol:

            case ProtocolEnum.S3:

                signed_url = self.kernel_planckster.generate_signed_url(source_data=source_data) 
                
                self.logger.info(f"{job_id}: Uploading video to object store")

                self.file_repository.public_upload(signed_url, local_file_name)
                
                self.logger.info(
                f"{job_id}: Uploaded video to {signed_url}"
                )

                self.kernel_planckster.register_new_source_data(source_data=source_data)

            case ProtocolEnum.LOCAL:
                # If local, then we don't use kernel planckster at all
                # NOTE: local is deprecated
                self.file_repository.save_file_locally(
                file_to_save=local_file_name,
                source_data=source_data,
                file_type="video",
                )

        return source_data