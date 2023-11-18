from enum import Enum
from typing import List, TypeVar
from pydantic import BaseModel, Field, model_validator
from datetime import datetime

class BaseJobState(Enum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"

class DataSource(Enum):
    TWITTER = "twitter"
    TELEGRAM = "telegram"
    SENTINEL = "sentinel"
    AUGMENTED_DATA = "augmented_data"

class Protocol(Enum):
    S3 = "s3"
    ES = "es"
    LOCAL = "local"

class LFN(BaseModel):
    protocol: Protocol
    tracer_id: str
    job_id: int
    source: DataSource
    relative_path: str
    pfn: str | None = None

    @model_validator(mode="after")
    def create_pfn(self):
        self.pfn = generate_pfn(self.protocol, self.source, self.tracer_id, self.job_id, self.relative_path)
        return self
class BaseJob(BaseModel):
    id: int
    tracer_id: str = Field(description="A unique identifier to trace jobs across the SDA runtime.")
    created_at: datetime = datetime.now()
    heartbeat: datetime = datetime.now()
    name: str
    state: Enum = BaseJobState.CREATED
    messages: List[str] = []
    output_lfns: List[LFN] = []
    input_lfns: List[LFN] = []

    def touch(self) -> None:
        self.heartbeat = datetime.now()


TBaseJob = TypeVar("TBaseJob", bound=BaseJob)



def generate_lfn(protocol: Protocol, data_source: DataSource, tracer_id: str, job_id: int, relative_path: str ) -> LFN:
    pfn = generate_pfn(protocol, data_source, tracer_id, job_id, relative_path)
    lfn = LFN(
            protocol=Protocol.ES,
            tracer_id=tracer_id,
            job_id=job_id,
            source=DataSource.TWITTER,
            relative_path=f"{id}/data2_climate.csv",
    )
    return lfn

def generate_pfn(protocol: Protocol, data_source: DataSource, tracer_id: str, job_id: int, relative_path: str) -> str:
    pfn: list[str] = []
    if protocol == Protocol.ES:
        pfn.append("http://localhost:9200")
    elif protocol == Protocol.S3:
        pfn.append("http://localhost:9000")
    elif protocol == Protocol.LOCAL:
        pfn.append("data")

    pfn.append(tracer_id)
    pfn.append(data_source.value)
    pfn.append(str(job_id))
    pfn.append(relative_path)

    return "/".join(pfn)

