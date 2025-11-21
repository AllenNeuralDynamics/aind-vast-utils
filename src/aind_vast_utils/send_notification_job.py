"""Module to send VAST notification"""

import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import List, Optional, Tuple

import awswrangler as wr
import pandas as pd
import requests
from aind_settings_utils.aws import SecretsManagerBaseSettings
from jinja2 import Template
from pandas import DataFrame
from pydantic import Field
from pydantic_settings import SettingsConfigDict

logging.basicConfig(level=logging.INFO)


class JobSettings(
    SecretsManagerBaseSettings,
    cli_parse_args=True,
    cli_ignore_unknown_args=True,
):
    """Settings needed to run SendNotificationJob"""

    # noinspection SpellCheckingInspection
    model_config = SettingsConfigDict(env_prefix="VAST_")
    tables_location: str = Field(
        ...,
        title="Tables Location",
        description=(
            "Location of tables. Can be local directory or S3 location."
        ),
    )
    alert_url: Optional[str] = Field(
        default=None,
        title="Alert URL",
        description="Endpoint to send the alert to.",
    )
    report_date: Optional[date] = Field(
        default_factory=lambda: datetime.now(UTC).date,
        title="Report Date",
        description="Date of the reported data.",
    )


class SendNotificationJob:
    """
    Job to send notifications to an endpoint.
    """

    def __init__(self, job_settings: JobSettings):
        """Class constructor."""
        self.job_settings = job_settings

    def _get_table(self, table_name: str) -> pd.DataFrame:
        """Get a table from either local directory or S3."""
        if self.job_settings.tables_location.startswith("s3://"):
            path = f"{self.job_settings.tables_location}/{table_name}"
            report_date = self.job_settings.report_date
            report_year = report_date.year
            # my_filter = lambda x: x["report_year"] == str(report_year) and x[
            #     "report_date"
            # ] == str(report_date)
            df = wr.s3.read_parquet(
                path,
                dataset=True,
                partition_filter=lambda x: x["report_year"] == str(report_year)
                and x["report_date"] == str(report_date),
            )
        else:
            path = (
                Path(self.job_settings.tables_location) / f"{table_name}.csv"
            )
            df = pd.read_csv(path)
        return df

    @staticmethod
    def _format_quota_table(quota_df: DataFrame) -> DataFrame:
        """Reformat the quota table"""
        problem_quota_df = quota_df[
            quota_df["state"].str.upper() != "OK"
        ].copy()
        problem_quota_df = problem_quota_df[
            [
                "path",
                "state",
                "used_capacity",
                "soft_limit",
                "hard_limit",
                "percent_capacity",
            ]
        ]
        problem_quota_df["soft_tb"] = problem_quota_df["soft_limit"] / (
            1024**4
        )
        problem_quota_df["hard_tb"] = problem_quota_df["hard_limit"] / (
            1024**4
        )
        problem_quota_df["used_capacity_tb"] = problem_quota_df[
            "used_capacity"
        ] / (1024**4)
        problem_quota_df.rename(
            columns={
                "path": "Path",
                "state": "State",
                "soft_tb": "Soft Limit (TiB)",
                "hard_tb": "Hard Limit (TiB)",
                "percent_capacity": "Percent Capacity",
                "used_capacity_tb": "Used Capacity (TiB)",
            },
            inplace=True,
        )
        return problem_quota_df[
            [
                "Path",
                "State",
                "Used Capacity (TiB)",
                "Soft Limit (TiB)",
                "Hard Limit (TiB)",
                "Percent Capacity",
            ]
        ]

    @staticmethod
    def top_capacity_table(df: DataFrame, path: str) -> DataFrame:
        """
        Filters the capacity table info to pull the top 5 folders and their
        top 3 subfolders.
        """
        top_df = (
            df[(df["parent"] == path) & (df["is_small_folders"].eq(False))]
            .sort_values(by="logical", ascending=False)
            .head(5)
        )
        top_df["sort_key"] = range(1, len(top_df) + 1)
        top_df["sort_key2"] = 0
        sort_key_map = pd.Series(
            top_df["sort_key"].values, index=top_df["path"]
        ).to_dict()
        top_folder_names = top_df["path"]
        sub_folders_df = df[df["parent"].isin(top_folder_names)].sort_values(
            by="logical", ascending=False
        )
        sub_folders_df["sort_key"] = sub_folders_df["parent"].map(sort_key_map)
        sub_folders_df["sort_key2"] = range(1, len(sub_folders_df) + 1)
        sub_folders_df = sub_folders_df.groupby("sort_key").head(3)
        concat_df = pd.concat([top_df, sub_folders_df])
        concat_df["logical_tb"] = concat_df["logical"] / (1024**4)
        concat_df = concat_df.sort_values(
            by=["sort_key", "sort_key2"], ascending=[True, True]
        )
        output_df = concat_df[["path", "logical_tb"]]
        output_df = output_df.rename(
            columns={"path": "Path", "logical_tb": "Logical TiB"}
        )
        return output_df

    @staticmethod
    def _format_tables_as_html(
        capacity_dfs: List[Tuple[str, str]], problem_quotas: str
    ):
        """Formats dfs to html"""

        template_string = """
        <div><p>We have reached a limit for data storage on VAST</p></div>
        <hr style="border-top: dashed 2px;">
        <div>
        {{ quotas_table | safe }}
        </div>
        {% for row in cap_tables %}
        <div>
        <p> {{ row[0] }} </p>
        {{ row[1] | safe }}
        </div>
        {% endfor %}
        <div>
        <p>
        DISCLAIMER:
        These are numbers estimated by VAST using statistical sampling.
        </p>
        </div>
        """
        template = Template(template_string)

        return template.render(
            quotas_table=problem_quotas, cap_tables=capacity_dfs
        )

    def send_notification(self, html_body: str):
        """Send a notification."""
        webhook_response = requests.post(
            self.job_settings.alert_url,
            json={"text": html_body},
            headers={"Content-Type": "application/json"},
            verify=False,
        )
        webhook_response.raise_for_status()

    def run_job(self):
        """Read quotas and capacity tables, send a notification if needed."""
        quota_df = self._get_table("quota")
        problem_quota_df = self._format_quota_table(quota_df)
        problem_paths = problem_quota_df["Path"]
        if len(problem_paths) > 0:
            dfs_to_report = []
            capacity_df = self._get_table("capacity")
            for problem_path in sorted(problem_paths):
                dfs_to_report.append(
                    (
                        problem_path,
                        self.top_capacity_table(
                            capacity_df, problem_path
                        ).to_html(index=False),
                    )
                )
            html_str = self._format_tables_as_html(
                dfs_to_report, problem_quota_df.to_html(index=False)
            )
            self.send_notification(html_str)
        else:
            logging.info("All quotas good.")
            logging.info(quota_df.to_string(index=False))


if __name__ == "__main__":
    if len(sys.argv[1:]) == 2 and sys.argv[1] == "--job-settings":
        main_job_settings = JobSettings.model_validate_json(sys.argv[2])
    else:
        # noinspection PyArgumentList
        main_job_settings = JobSettings()
    main_job = SendNotificationJob(job_settings=main_job_settings)
    main_job.run_job()
