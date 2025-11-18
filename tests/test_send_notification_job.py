"""Tests send_notification_job module."""

import os
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from requests import Response

from aind_vast_utils.send_notification_job import (
    JobSettings,
    SendNotificationJob,
)

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"


class TestSendNotificationJob(unittest.TestCase):
    """Tests SendNotificationJob class."""

    @classmethod
    @patch.dict(os.environ, {}, clear=True)
    def setUpClass(cls):
        """Set up class with mocked responses and default test job."""

        job_settings = JobSettings(
            tables_location=str(RESOURCES_DIR),
            alert_url="www.example.com/alert",
            report_date=date.fromisoformat("2025-11-12"),
        )
        job_settings2 = JobSettings(
            tables_location=str(RESOURCES_DIR / "example_csvs"),
            alert_url="www.example.com/alert",
            report_date=date.fromisoformat("2025-11-12"),
        )
        cls.job = SendNotificationJob(job_settings=job_settings)
        cls.other_job = SendNotificationJob(job_settings=job_settings2)

    @patch("awswrangler.s3.read_parquet")
    def test_get_table(self, mock_wrangler: MagicMock):
        """Tests _get_table method."""

        table = self.job._get_table(table_name="quota")
        self.assertIsInstance(table, pd.DataFrame)
        mock_wrangler.assert_not_called()

    @patch("awswrangler.s3.read_parquet")
    def test_get_table_from_s3(self, mock_wrangler: MagicMock):
        """Tests _get_table method when tables_location set to S3."""
        new_job_settings = self.job.job_settings.model_copy(
            deep=True, update={"tables_location": "s3://example/tables"}
        )
        new_job = SendNotificationJob(job_settings=new_job_settings)
        new_job._get_table(table_name="quota")
        mock_wrangler.assert_called_once()

    def test_top_capacity_table(self):
        """Tests top_capacity_table method"""

        table = self.job._get_table(table_name="capacity")
        df = self.job.top_capacity_table(table, path="/aind/scratch")
        self.assertEqual(["Path", "Logical TiB"], list(df.columns))

    def test_format_tables_as_html(self):
        """Tests _format_tables_as_html method"""
        table = self.job._get_table(table_name="capacity")
        df = self.job.top_capacity_table(table, path="/aind/scratch")
        quota_df = self.job._get_table(table_name="quota")
        problem_quota_df = self.job._format_quota_table(quota_df)
        html_body = self.job._format_tables_as_html(
            capacity_dfs=[("/aind/scratch", df.to_html(index=False))],
            problem_quotas=problem_quota_df.to_html(index=False),
        )
        self.assertIn(
            "We have reached a limit for data storage on VAST", html_body
        )

    @patch("requests.post")
    def test_run_job_with_notification(self, mock_post: MagicMock):
        """Tets run_job when a notification is sent."""
        mock_response = Response()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        self.job.run_job()
        mock_post.assert_called_once()

    @patch("requests.post")
    def test_run_job_with_all_good(self, mock_post: MagicMock):
        """Tests run_job when all is good."""
        with self.assertLogs(level="INFO") as captured:
            self.other_job.run_job()
        self.assertEqual(2, len(captured.output))
        mock_post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
