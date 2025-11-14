"""Tests configs module."""

import os
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

from aind_vast_utils.configs import JobSettings


class TestJobSettings(unittest.TestCase):
    """Tests JobSettings class."""

    @patch.dict(
        os.environ,
        {
            "VAST_ADDRESS": "example.com",
            "VAST_USER": "user",
            "VAST_PASSWORD": "password",
            "VAST_REPORT_DATETIME": "2025-11-12T00:00:00Z",
        },
        clear=True,
    )
    def test_model_construction(self):
        """Tests model can be constructed from env vars."""

        # noinspection PyArgumentList
        job_settings = JobSettings()
        self.assertEqual(
            datetime(2025, 11, 12, 0, 0, 0, tzinfo=UTC),
            job_settings.report_datetime,
        )
        self.assertEqual("example.com", job_settings.address)
        self.assertEqual("user", job_settings.user)
        self.assertEqual("password", job_settings.password.get_secret_value())
        self.assertIsNone(job_settings.output_location)
        self.assertEqual(["/aind/scratch", "/aind/stage"], job_settings.paths)


if __name__ == "__main__":
    unittest.main()
