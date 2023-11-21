import os
from typing import Any, Callable, Dict, List, TypedDict
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from app.sdk.kernel_plackster_gateway import KernelPlancksterGateway
from app.sdk.minio_gateway import MinIORepository

from app.sdk.models import LFN, Protocol


class JobManagerFastAPIRouter:
    def __init__(self, app, worker: Callable):
        self.app = app
        self.router = APIRouter()
        self.register_endpoints()
        self.app.include_router(self.router)
        self.worker = worker
        MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
        MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
        MINIO_HOST = os.getenv("MINIO_HOST")
        MINIO_PORT = os.getenv("MINIO_PORT")
        MINIO_BUCKET = os.getenv("MINIO_BUCKET")
        STORAGE_PROTOCOL_CONFIG = os.getenv("STORAGE_PROTOCOL", "S3")

        self.STORAGE_PROTOCOL = Protocol(STORAGE_PROTOCOL_CONFIG.lower())
        if self.STORAGE_PROTOCOL == Protocol.S3:
            if not (
                MINIO_ACCESS_KEY
                and MINIO_SECRET_KEY
                and MINIO_HOST
                and MINIO_PORT
                and MINIO_BUCKET
            ):
                raise ValueError(
                    "Environment Variables MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_HOST and MINIO_PORT and MINIO_BUCKET must be set."
                )
            self.minio_repository = MinIORepository(
                host=MINIO_HOST,
                port=MINIO_PORT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
            )

        KP_HOST = os.getenv("KERNEL_PLANKSTER_HOST", "http://localhost")
        KP_PORT = os.getenv("KERNEL_PLANKSTER_PORT", "8000")

        if not (KP_HOST and KP_PORT):
            raise ValueError(
                "Environment Variables KERNEL_PLANKSTER_HOST and KERNEL_PLANKSTER_PORT must be set."
            )
        if "http" not in KP_HOST:
            raise ValueError(
                "Environment Variable KERNEL_PLANKSTER_HOST must start with http:// or https://"
            )
        self.kernel_plankster_gateway = KernelPlancksterGateway(
            host=KP_HOST, port=KP_PORT
        )

    def register_endpoints(self):
        @self.router.get("/job")
        def list_all_jobs():
            job_manager = self.app.job_manager  # type: ignore
            return job_manager.list_jobs()

        @self.router.post("/job")
        def create_job(
            tracer_id: str,
            job_args: Dict[str, Any],
            input_lfns: List[LFN] | None = None,
        ):
            job_manager = self.app.job_manager  # type: ignore
            job = job_manager.create_job(tracer_id, job_args)  # type: ignore
            return job

        @self.router.get("/job/{job_id}")
        def get_job(job_id: int):
            job_manager = self.app.job_manager  # type: ignore
            job = job_manager.get_job(job_id)
            return job

        @self.router.get("/job/{job_id}/start")
        def start_job(job_id: int, background_tasks: BackgroundTasks):
            job_manager = self.app.job_manager
            try:
                job = job_manager.get_job(job_id)
            except KeyError:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            if self.STORAGE_PROTOCOL == Protocol.S3:
                try:
                    bucket = self.minio_repository.bucket
                    self.minio_repository.create_bucket_if_not_exists(bucket)
                except Exception as e:
                    raise HTTPException(
                        status_code=500, detail=f"Failed to connect to MinIO: {e}"
                    )
            pong = self.kernel_plankster_gateway.ping()
            if not pong:
                raise HTTPException(
                    status_code=500, detail="Failed to connect to Kernel Plankster"
                )
            background_tasks.add_task(
                self.worker,
                job=job,
                kernel_planckster=self.kernel_plankster_gateway,
                minio_repository=self.minio_repository,
            )
