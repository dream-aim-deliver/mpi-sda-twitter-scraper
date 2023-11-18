from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List

from app.sdk.models import BaseJob, TBaseJob


class BaseJobManager(ABC, Generic[TBaseJob]):
    def __init__(self) -> None:
        self._jobs: Dict[int, TBaseJob] = {} 
        self._nonce = 0
    
    @property
    def jobs(self) -> Dict[int, TBaseJob]:
        return self._jobs
    
    @property
    def nonce(self) -> int:
        self._nonce = self._nonce + 1
        return self._nonce
    
    @abstractmethod
    def make(self, *args: Any, **kwargs: Any) -> TBaseJob:
        raise NotImplementedError("make method must be implemented in a subclass.")


    def create_job(self, tracer_id: str, *args: Any, **kwargs: Any) -> BaseJob:
        id = self.nonce
        job = self.make(tracer_id=tracer_id, id=id, **kwargs)
        self.jobs[job.id] = job # type: ignore
        return job        
    
    def get_job(self, job_id: int) -> TBaseJob:
        return self.jobs[job_id]
    
    def list_jobs(self) -> List[TBaseJob]:
        return list(self._jobs.values())