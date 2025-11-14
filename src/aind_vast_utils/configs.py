"""Module for settings to connect to VAST"""

from datetime import UTC, datetime
from typing import List, Optional

from aind_settings_utils.aws import SecretsManagerBaseSettings
from pydantic import Field, SecretStr
from pydantic_settings import SettingsConfigDict


class JobSettings(
    SecretsManagerBaseSettings,
    cli_parse_args=True,
    cli_ignore_unknown_args=True,
):
    """Settings needed to run CompileMetricsJob"""

    # noinspection SpellCheckingInspection
    model_config = SettingsConfigDict(env_prefix="VAST_")
    # noinspection SpellCheckingInspection
    address: str = Field(
        ...,
        title="Address",
        description="Address name of VAST cluster.",
    )
    # TODO: API token might be better to authenticate with
    user: str = Field(..., title="User", description="Username.")
    password: SecretStr = Field(..., title="Password", description="Password.")
    paths: List[str] = Field(
        default=["/aind/scratch", "/aind/stage"],
        title="Paths",
        description="Top folders to run inspection against",
    )
    output_location: Optional[str] = Field(
        default=None,
        title="Output Location",
        description=(
            "Location to write parquet file to. Will simply log table if None."
        ),
    )
    report_datetime: Optional[datetime] = Field(
        default_factory=lambda: datetime.now(UTC)
    )
