from typing import Any, Dict, List
import os

from app.sdk.models import BaseJob


class BaseJobManager:
    def __init__(self) -> None:
        self._jobs: Dict[int, BaseJob] = {}
        self._nonce = 0

    @property
    def name(self) -> str:
        return os.getenv("JOB_MANAGER_NAME", "default")

    @property
    def jobs(self) -> Dict[int, BaseJob]:
        return self._jobs

    @property
    def nonce(self) -> int:
        self._nonce = self._nonce + 1
        return self._nonce

    def create_job(
        self, tracer_id: str, job_args: Dict[str, Any], *args: Any, **kwargs: Any
    ) -> BaseJob:
        id = self.nonce
        job = BaseJob(
            id=id,
            name=f"{self.name}-{id}",
            tracer_id=tracer_id,
            args=job_args,
        )

        self.jobs[job.id] = job  # type: ignore
        return job

    def get_job(self, job_id: int) -> BaseJob:
        return self.jobs[job_id]

    def list_jobs(self) -> List[BaseJob]:
        return list(self._jobs.values())
