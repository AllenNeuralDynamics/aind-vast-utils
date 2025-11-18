"""Tests compile_metrics_job module."""

import json
import os
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from pydantic import SecretStr
from pydantic_core import TzInfo

from aind_vast_utils.compile_metrics_job import CompileMetricsJob, JobSettings
from aind_vast_utils.models import Capacity, CapacityData, Quota

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"


class TestCompileMetricsJob(unittest.TestCase):
    """Tests CompileMetricsJob class."""

    @classmethod
    @patch.dict(os.environ, {}, clear=True)
    def setUpClass(cls):
        """Set up class with mocked responses and default test job."""

        with open(RESOURCES_DIR / "capacity_response.json", "r") as f:
            capacity_response = json.load(f)

        with open(RESOURCES_DIR / "quotas_response.json", "r") as f:
            quotas_response = json.load(f)

        cls.patch_vast_client = patch(
            "aind_vast_utils.compile_metrics_job.VASTClient"
        )
        cls.mock_vast_client = cls.patch_vast_client.start()
        cls.mock_vast_client.return_value.capacity.get.return_value = (
            capacity_response
        )
        cls.mock_vast_client.return_value.quotas.get.return_value = (
            quotas_response
        )
        job_settings = JobSettings(
            address="example.com",
            user="user",
            password=SecretStr("password"),
            report_datetime=datetime(2025, 11, 12, 0, 0, 0).astimezone(
                tz=timezone.utc
            ),
            paths=["/aind/scratch"],
        )
        job = CompileMetricsJob(job_settings=job_settings)
        cls.job = job

    @classmethod
    def tearDownClass(cls):
        """Tear down class level patcher."""
        cls.patch_vast_client.stop()

    def test_get_capacity(self):
        """Tests get_capacity method."""

        capacity = self.job._get_capacity(
            path="/aind/scratch", sort_key="usable"
        )
        expected_capacity = Capacity(
            details=[
                (
                    "/aind/scratch",
                    CapacityData(
                        data=[
                            318535641364598,
                            276370399582092,
                            771249318149374,
                        ],
                        parent="/aind",
                        percent=100.0,
                        average_atime=datetime(2024, 12, 17, 9, 55),
                    ),
                ),
                (
                    "/aind/scratch/ophys",
                    CapacityData(
                        data=[31436537147351, 26412466407190, 275667807998781],
                        parent="/aind/scratch",
                        percent=9.87,
                        average_atime=datetime(2025, 2, 6, 23, 44),
                    ),
                ),
            ],
            keys=["usable", "unique", "logical"],
            time=datetime(2025, 11, 12, 18, 59),
            sort_key="usable",
            root_data=[3128954951249558, 2689008553951633, 5441825959464217],
            small_folders=[
                (
                    "/aind/scratch/abc",
                    CapacityData(
                        data=[5143983132, 5018378864, 15452267346],
                        parent="/aind/scratch",
                        percent=0.0,
                        average_atime=datetime(2025, 5, 30, 17, 28),
                    ),
                ),
                (
                    "/aind/scratch/def",
                    CapacityData(
                        data=[4139968368, 1272458632, 30855765913],
                        parent="/aind/scratch",
                        percent=0.0,
                        average_atime=datetime(2024, 2, 3, 5, 43),
                    ),
                ),
            ],
        )
        self.assertEqual(expected_capacity, capacity)

    def test_get_quota(self):
        """Tests _get_quota method."""

        quota = self.job._get_quota(path="/aind/scratch")
        expected_quota = Quota(
            id=123,
            guid="1a11a1aa-aa11-1a11-11a1-1aa11aaa11aa",
            name="aind_scratch",
            url="https://example.com/api/quotas/153",
            title="aind_scratch",
            state="SOFT_EXCEEDED",
            path="/aind/scratch",
            grace_period=None,
            soft_limit=747667906887680,
            hard_limit=786150813859840,
            soft_limit_inodes=None,
            hard_limit_inodes=None,
            used_inodes=87589553,
            sync_state="SYNCHRONIZED",
            used_capacity=775337561472891,
            used_effective_capacity=775337561472891,
            used_capacity_tb=705.165,
            pretty_state="SOFT_EXCEEDED",
            used_effective_capacity_tb=705.165,
            cluster="VAST-CLUSTER",
            cluster_id=1,
            tenant_id=1,
            internal=False,
            pretty_grace_period=None,
            pretty_grace_period_expiration=None,
            time_to_block=None,
            default_user_quota=None,
            default_group_quota=None,
            system_id=345,
            is_user_quota=False,
            num_exceeded_users=0,
            num_blocked_users=0,
            enable_alarms=True,
            default_email="example@example.com",
            last_user_quotas_update=datetime(
                2025, 9, 3, 18, 12, 21, 781741, tzinfo=TzInfo(0)
            ),
            percent_inodes=None,
            percent_capacity=98,
            enable_email_providers=True,
            used_limited_capacity=775337561472891,
            tenant_name="default",
        )
        self.assertEqual(expected_quota, quota)

    def test_map_to_capacity_table_rows(self):
        """Tests _map_to_capacity_table_rows method."""

        path = "/aind/scratch"
        capacity_info = self.job._get_capacity(path=path, sort_key="usable")
        rows = self.job._map_to_capacity_table_rows(
            capacity_info=capacity_info,
        )
        self.assertEqual(4, len(rows))
        self.assertEqual("/aind/scratch/ophys", rows[1].path)

    def test_map_to_quota_table_rows(self):
        """Tests _map_to_quota_table_rows method."""

        path = "/aind/scratch"
        quota_info = self.job._get_quota(path=path)
        rows = self.job._map_to_quota_table_rows(quotas=[quota_info])
        self.assertEqual(98, rows[0].percent_capacity)

    def test_map_rows_to_dataframe(self):
        """Tests _map_rows_to_dataframe method"""
        path = "/aind/scratch"
        capacity_info = self.job._get_capacity(path=path, sort_key="usable")
        rows = self.job._map_to_capacity_table_rows(
            capacity_info=capacity_info,
        )
        df = self.job._map_rows_to_dataframe(rows=rows)
        self.assertEqual(2025, df.loc[0, "report_year"])
        self.assertEqual(date(2025, 11, 12), df.loc[0, "report_date"])

    @patch("pandas.DataFrame.to_csv")
    @patch("awswrangler.s3.to_parquet")
    @patch("builtins.print")
    def test_write_report_print(
        self,
        mock_print: MagicMock,
        mock_awswrangler_s3_to_parquet: MagicMock,
        mock_pandas_df_to_csv: MagicMock,
    ):
        """Tests write report function when no output location set."""
        path = "/aind/scratch"
        capacity_info = self.job._get_capacity(path=path, sort_key="usable")
        rows = self.job._map_to_capacity_table_rows(
            capacity_info=capacity_info,
        )
        df = self.job._map_rows_to_dataframe(rows=rows)
        self.job._write_report(df=df, report_name="capacity")
        mock_pandas_df_to_csv.assert_not_called()
        mock_awswrangler_s3_to_parquet.assert_not_called()
        mock_print.assert_called()

    @patch("pandas.DataFrame.to_csv")
    @patch("awswrangler.s3.to_parquet")
    @patch("builtins.print")
    def test_write_report_csv(
        self,
        mock_print: MagicMock,
        mock_awswrangler_s3_to_parquet: MagicMock,
        mock_pandas_df_to_csv: MagicMock,
    ):
        """Tests write report function when local output location set."""
        job_settings = self.job.job_settings.model_copy(
            deep=True, update={"output_location": "."}
        )
        new_job = CompileMetricsJob(job_settings=job_settings)
        path = "/aind/scratch"
        capacity_info = new_job._get_capacity(path=path, sort_key="usable")
        rows = new_job._map_to_capacity_table_rows(
            capacity_info=capacity_info,
        )
        df = new_job._map_rows_to_dataframe(rows=rows)
        new_job._write_report(df=df, report_name="capacity")
        mock_pandas_df_to_csv.assert_called()
        mock_awswrangler_s3_to_parquet.assert_not_called()
        mock_print.assert_not_called()

    @patch("pandas.DataFrame.to_csv")
    @patch("awswrangler.s3.to_parquet")
    @patch("builtins.print")
    def test_write_report_s3(
        self,
        mock_print: MagicMock,
        mock_awswrangler_s3_to_parquet: MagicMock,
        mock_pandas_df_to_csv: MagicMock,
    ):
        """Tests write report function when s3 output location set."""
        job_settings = self.job.job_settings.model_copy(
            deep=True, update={"output_location": "s3://example/path"}
        )
        new_job = CompileMetricsJob(job_settings=job_settings)
        path = "/aind/scratch"
        capacity_info = new_job._get_capacity(path=path, sort_key="usable")
        rows = new_job._map_to_capacity_table_rows(
            capacity_info=capacity_info,
        )
        df = new_job._map_rows_to_dataframe(rows=rows)
        new_job._write_report(df=df, report_name="capacity")
        mock_pandas_df_to_csv.assert_not_called()
        mock_awswrangler_s3_to_parquet.assert_called()
        mock_print.assert_not_called()

    @patch(
        "aind_vast_utils.compile_metrics_job.CompileMetricsJob._write_report"
    )
    def test_run_job(self, mock_write_report: MagicMock):
        """Tests run job method."""

        self.job.run_job()
        mock_write_report.assert_called()


if __name__ == "__main__":
    unittest.main()
