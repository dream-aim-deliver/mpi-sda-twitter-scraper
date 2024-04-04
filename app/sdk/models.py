from enum import Enum
from typing import List, TypeVar
from pydantic import BaseModel, Field
from datetime import datetime


class BaseJobState(Enum):
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"

class ProtocolEnum(Enum):
    """
    The storage protocol to use for a file.

    Attributes:
    - S3: S3
    - LOCAL: Local  @deprecated
    """
    S3 = "s3"
    LOCAL = "local"


class KernelPlancksterSourceData(BaseModel):
    """
    Synchronize this with Kernel Planckster's SourceData model, so that this client generates valid requests.

    @attr name: the name of the source data to register as metadata
    @attr protocol: the protocol to use to store the source data
    @attr relative_path: the relative path to store the source data in the storage system
    """
    name: str
    protocol: ProtocolEnum
    relative_path: str

    def to_json(cls) -> str:
        """
        Dumps the model to a json formatted string. Wrapper around pydantic's model_dump_json method: in case they decide to deprecate it, we only refactor here.
        """
        return cls.model_dump_json()

    def __str__(self) -> str:
        return self.to_json()

    @classmethod
    def from_json(cls, json_str: str) -> "KernelPlancksterSourceData":
        """
        Loads the model from a json formatted string. Wrapper around pydantic's model_validate_json method: in case they decide to deprecate it, we only refactor here.
        """
        return cls.model_validate_json(json_data=json_str)


class BaseJob(BaseModel):
    """
    NOTE: deprecated.
    """
    id: int
    tracer_id: str = Field(
        description="A unique identifier to trace jobs across the SDA runtime."
    )
    created_at: datetime = datetime.now()
    heartbeat: datetime = datetime.now()
    name: str
    args: dict = {}
    state: Enum = BaseJobState.CREATED
    messages: List[str] = []
    output_source_data_list: List[KernelPlancksterSourceData] = []
    input_source_data_list: List[KernelPlancksterSourceData] = []

    def touch(self) -> None:
        self.heartbeat = datetime.now()


TBaseJob = TypeVar("TBaseJob", bound=BaseJob)


class JobOutput(BaseModel):
    """
    This class is used to represent the output of a scraper job.

    Attributes:
    - job_state: BaseJobState
    - trace_id: str
    - source_data_list: List[KernelPlancksterSourceData] | None
    """

    job_state: BaseJobState
    tracer_id: str
    source_data_list: List[KernelPlancksterSourceData] | None
