"""Models of VAST API responses. Schemas are from their API docs."""

from datetime import datetime
from typing import List, Optional, Tuple

from pydantic import BaseModel, Field


class CapacityData(BaseModel):
    """Expected shape of the capacity data."""

    data: List[int] = Field(
        ...,
        description=(
            "A list of the capacity usage in bytes. The numbers correspond "
            "to the list in the Capacity.keys entries."
        ),
    )
    parent: str = Field(..., description="Parent folder.")
    percent: float = Field(
        ..., description="Percent of parent folder that this folder uses."
    )
    average_atime: Optional[datetime] = Field(
        default=None,
        description=(
            "A weighted average value calculated from the atime values of the "
            "files in the directory, with weighting based on file size. "
            "atime is a file attribute that tells you when a file was last "
            "accessed."
        ),
    )


class Capacity(BaseModel):
    """Response returned from VAST API"""

    details: List[Tuple[str, CapacityData]] = Field(...)
    keys: List[str] = Field(
        ...,
        description=(
            "The data field is a list of integers. This field is what the "
            "integers represent."
        ),
    )
    time: datetime = Field(...)
    sort_key: str
    root_data: List[int]
    small_folders: List[Tuple[str, CapacityData]]


class Quota(BaseModel):
    """Response returned from VAST API for quota"""

    id: Optional[int] = Field(default=None)
    guid: Optional[str] = Field(default=None)
    name: Optional[str] = Field(default=None)
    url: Optional[str] = Field(default=None)
    title: Optional[str] = Field(default=None)
    state: Optional[str] = Field(default=None)
    path: Optional[str] = Field(default=None)
    grace_period: Optional[str] = Field(default=None)
    soft_limit: Optional[int] = Field(default=None)
    hard_limit: Optional[int] = Field(default=None)
    soft_limit_inodes: Optional[int] = Field(default=None)
    hard_limit_inodes: Optional[int] = Field(default=None)
    used_inodes: Optional[int] = Field(default=None)
    sync_state: Optional[str] = Field(default=None)
    used_capacity: Optional[int] = Field(default=None)
    used_effective_capacity: Optional[int] = Field(default=None)
    used_capacity_tb: Optional[float] = Field(default=None)
    pretty_state: Optional[str] = Field(default=None)
    used_effective_capacity_tb: Optional[float] = Field(default=None)
    cluster: Optional[str] = Field(default=None)
    cluster_id: Optional[int] = Field(default=None)
    tenant_id: Optional[int] = Field(default=None)
    internal: Optional[bool] = Field(default=None)
    pretty_grace_period: Optional[str] = Field(default=None)
    pretty_grace_period_expiration: Optional[str] = Field(default=None)
    time_to_block: Optional[str] = Field(default=None)
    default_user_quota: Optional[dict] = Field(default=None)
    default_group_quota: Optional[str] = Field(default=None)
    system_id: Optional[int] = Field(default=None)
    is_user_quota: Optional[bool] = Field(default=None)
    num_exceeded_users: Optional[int] = Field(default=None)
    num_blocked_users: Optional[int] = Field(default=None)
    enable_alarms: Optional[bool] = Field(default=None)
    default_email: Optional[str] = Field(default=None)
    last_user_quotas_update: Optional[datetime] = Field(default=None)
    percent_inodes: Optional[int] = Field(default=None)
    percent_capacity: Optional[int] = Field(default=None)
    enable_email_providers: Optional[bool] = Field(default=None)
    used_limited_capacity: Optional[int] = Field(default=None)
    tenant_name: Optional[str] = Field(default=None)


class CapacityTableRow(BaseModel):
    """Table row used to generate reports."""

    report_datetime: datetime
    path: str
    is_small_folders: bool
    usable: int
    unique: int
    logical: int
    parent: str
    percent: float


class QuotaTableRow(BaseModel):
    """Table row used to generate reports."""

    report_datetime: datetime
    path: str
    state: str
    used_capacity: int
    soft_limit: int
    hard_limit: int
    percent_capacity: int
