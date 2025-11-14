"""
Module for compiling and reporting metrics.
"""

import logging
import sys
from typing import List

import awswrangler as wr
import pandas as pd
from vastpy import VASTClient

from aind_vast_utils.configs import JobSettings
from aind_vast_utils.models import (
    Capacity,
    CapacityTableRow,
    Quota,
    QuotaTableRow,
)

logging.basicConfig(level=logging.INFO)


class CompileMetricsJob:
    """
    Job to compile metrics about VAST cluster and write a report
    """

    def __init__(self, job_settings: JobSettings):
        """Class constructor."""
        self.job_settings = job_settings
        self.vast_client = VASTClient(
            user=job_settings.user,
            password=job_settings.password.get_secret_value(),
            address=job_settings.address,
        )

    def _get_capacity(self, path: str, sort_key: str = "logical") -> Capacity:
        """Get capacity info for VAST cluster."""

        response: dict = self.vast_client.capacity.get(
            path=path,
            type=sort_key,
        )
        return Capacity.model_validate(response)

    def _get_quota(self, path: str) -> Quota:
        """Get quota info for VAST cluster."""

        response: List[dict] = self.vast_client.quotas.get(path=path)
        return Quota.model_validate(response[0])

    def _map_to_quota_table_rows(
        self, quotas: List[Quota]
    ) -> List[QuotaTableRow]:
        """Get quota info for VAST cluster."""
        all_rows = []
        for quota in quotas:
            row = QuotaTableRow(
                report_datetime=self.job_settings.report_datetime,
                path=quota.path,
                state=quota.state,
                used_capacity=quota.used_capacity,
                soft_limit=quota.soft_limit,
                hard_limit=quota.hard_limit,
                percent_capacity=quota.percent_capacity,
            )
            all_rows.append(row)
        return all_rows

    def _map_to_capacity_table_rows(
        self, capacity_info: Capacity
    ) -> List[CapacityTableRow]:
        """Map Capacity response to a CapacityTableRow"""
        key_map = dict(
            [(item, index) for index, item in enumerate(capacity_info.keys)]
        )
        main_folders = []
        for capacity_data in capacity_info.details:
            folder_name = capacity_data[0]
            folder_data = capacity_data[1]
            row = CapacityTableRow(
                report_datetime=self.job_settings.report_datetime,
                path=folder_name,
                is_small_folders=False,
                usable=folder_data.data[key_map["usable"]],
                unique=folder_data.data[key_map["unique"]],
                logical=folder_data.data[key_map["logical"]],
                parent=folder_data.parent,
                percent=folder_data.percent,
            )
            main_folders.append(row)
        small_folders = []
        for capacity_data in capacity_info.small_folders:
            folder_name = capacity_data[0]
            folder_data = capacity_data[1]
            row = CapacityTableRow(
                report_datetime=self.job_settings.report_datetime,
                path=folder_name,
                is_small_folders=True,
                usable=folder_data.data[key_map["usable"]],
                unique=folder_data.data[key_map["unique"]],
                logical=folder_data.data[key_map["logical"]],
                parent=folder_data.parent,
                percent=folder_data.percent,
            )
            small_folders.append(row)
        main_folders.sort(key=lambda x: x.logical, reverse=True)
        small_folders.sort(key=lambda x: x.logical, reverse=True)
        all_folders = main_folders + small_folders
        return all_folders

    @staticmethod
    def _map_rows_to_dataframe(
        rows: List[CapacityTableRow] | List[QuotaTableRow],
    ) -> pd.DataFrame:
        """
        Map list of pydantic models to pandas DataFrame. Adds rows for year
        and date to partition data.
        """
        df = pd.DataFrame([row.model_dump() for row in rows])
        df["report_date"] = df["report_datetime"].dt.date
        df["report_year"] = df["report_datetime"].dt.year
        return df

    def _write_report(self, df: pd.DataFrame, report_name: str) -> None:
        """Write report to file."""
        if self.job_settings.output_location is None:
            print(report_name)
            print(df.to_string())
        elif self.job_settings.output_location.startswith("s3://"):
            output_location = (
                f"{self.job_settings.output_location}/{report_name}"
            )
            wr.s3.to_parquet(
                df=df,
                path=output_location,
                dataset=True,
                partition_cols=["report_year", "report_date"],
                mode="overwrite_partitions",
            )
        else:
            output_location = (
                f"{self.job_settings.output_location}/{report_name}.csv"
            )
            df.to_csv(output_location, index=False)

    def run_job(self):
        """Compile the metrics and generate a report."""

        all_capacity_rows = []
        all_quotas = []
        for path in self.job_settings.paths:
            capacity = self._get_capacity(path=path)
            capacity_rows = self._map_to_capacity_table_rows(
                capacity_info=capacity,
            )
            all_capacity_rows.extend(capacity_rows)
            quota = self._get_quota(path=path)
            all_quotas.append(quota)
        all_quota_rows = self._map_to_quota_table_rows(all_quotas)
        capacity_df = self._map_rows_to_dataframe(all_capacity_rows)
        quota_df = self._map_rows_to_dataframe(all_quota_rows)
        self._write_report(capacity_df, "capacity")
        self._write_report(quota_df, "quota")


if __name__ == "__main__":
    if len(sys.argv[1:]) == 2 and sys.argv[1] == "--job-settings":
        main_job_settings = JobSettings.model_validate_json(sys.argv[2])
    else:
        # noinspection PyArgumentList
        main_job_settings = JobSettings()
    main_job = CompileMetricsJob(job_settings=main_job_settings)
    main_job.run_job()
