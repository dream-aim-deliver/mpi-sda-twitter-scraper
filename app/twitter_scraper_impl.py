from typing import Any
from pydantic import field_validator
from app.sdk.job_manager import BaseJobManager
from app.sdk.models import LFN, BaseJob, BaseJobState, Protocol, DataSource
from enum import Enum

class TwitterScraperStage(Enum):
    UPLOADING_TO_ES = "uploading_to_es"

class TwitterScraperJob(BaseJob):
    state: BaseJobState | TwitterScraperStage = BaseJobState.CREATED

class TwitterScraperJobManager(BaseJobManager[TwitterScraperJob]):
    def __init__(self) -> None:
        super().__init__()
    
    def make(self, tracer_id: str, id: str, *args: Any, **kwargs: Any) -> TwitterScraperJob:
        job = TwitterScraperJob(
            id=int(id),
            tracer_id=tracer_id,
            name=f"twitter-{id}",
            
        )
        return job