import json
import os
import unittest
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyspark import SparkConf
from pyspark.sql import SparkSession

from aind_vast_utils.compile_s3_metrics import (
    CompileS3MetricsJob,
    JobSettings,
)

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

from pyspark.testing import assertDataFrameEqual  # noqa: E402

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"
RESPONSES_DIR = RESOURCES_DIR / "s3_inventory_examples"
CLS_REF = "aind_vast_utils.compile_s3_metrics.CompileS3MetricsJob"


class TestCompileS3MetricsJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        # There is a known issue when calling the assertDataFrameEqual function
        # which calls pyspark.pandas. The spark session is stopped in the
        # tearDownClass method
        warnings.filterwarnings(
            "ignore",
            category=ResourceWarning,
            message="unclosed <socket.socket",
        )

        with open(RESPONSES_DIR / "manifest.json", "r") as f:
            example_manifest = json.load(f)

        with open(RESPONSES_DIR / "example_docdb_response.json", "r") as f:
            example_docdb_response = json.load(f)

        with open(RESPONSES_DIR / "s3_list_response.json", "r") as f:
            example_s3_list_objects_response = json.load(f)

        cls.example_manifest = example_manifest
        cls.example_docdb_response = example_docdb_response
        cls.example_s3_list_objects_response = example_s3_list_objects_response
        cls.csv_file_location = RESPONSES_DIR / "example_inventory_info.csv"
        cls.expected_output_df_file = RESPONSES_DIR / "expected_output_df.json"
        cls.example_docdb_info = [
            ("SmartSPIM_782747_2025-03-24_17-26-26", "Project 1"),
            ("behavior_844782_2026-05-05_09-58-34", "Project 2"),
            ("behavior_781900_2025-06-04_13-35-13", "Project 2"),
            ("822683_2026-03-03_16-37-05_pr_2026-03-04_11-48-56", "Project 3"),
        ]
        job_settings = JobSettings(
            s3_inventory_bucket="inventory-bucket",
            s3_inventory_prefix="inventory-prefix",
            bucket="example-bucket",
            output_location="test_output_path",
            docdb_host="example.com",
            spark_configs={
                "spark.app.name": "S3InventoryMetricsTest",
                "spark.master": "local[1]",
                "spark.sql.shuffle.partitions": "1",
                "spark.driver.memory": "1g",
            },
        )
        spark_conf = SparkConf().setAll(
            list(job_settings.spark_configs.items())
        )
        cls.spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        cls.job = CompileS3MetricsJob(
            job_settings=job_settings, spark=cls.spark
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch("aind_vast_utils.compile_s3_metrics.MetadataDbClient")
    def test_get_docdb_info(self, mock_docdb_client: MagicMock):

        mock_docdb_client.return_value.retrieve_docdb_records.return_value = (
            self.example_docdb_response
        )
        docdb_info = self.job._get_docdb_info()
        expected_info = self.example_docdb_info
        self.assertEqual(docdb_info, expected_info)

    @patch("boto3.client")
    def test_get_latest_manifest(self, mock_boto_client: MagicMock):
        mock_s3_instance = MagicMock()
        mock_paginator = MagicMock()
        mock_boto_client.return_value = mock_s3_instance
        mock_s3_instance.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = (
            self.example_s3_list_objects_response
        )
        latest_manifest, report_date = self.job._get_latest_manifest()
        expected_latest_manifest = (
            "inventory-bucket/inventory-prefix/2026-06-07T01-00Z/manifest.json"
        )
        expected_values = (expected_latest_manifest, "2026-06-07T01-00Z")
        self.assertEqual((latest_manifest, report_date), expected_values)

    @patch("boto3.client")
    def test_get_latest_manifest_error(self, mock_boto_client: MagicMock):
        mock_s3_instance = MagicMock()
        mock_paginator = MagicMock()
        mock_boto_client.return_value = mock_s3_instance
        mock_s3_instance.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = []
        with self.assertRaises(FileNotFoundError):
            self.job._get_latest_manifest()

    @patch("pyspark.sql.DataFrameReader.json")
    def test_get_inventory_list(self, mock_spark_read_json: MagicMock):
        mock_df = MagicMock()
        mock_json_df = MagicMock()
        expected_json = json.dumps(self.example_manifest)
        mock_json_df.first.return_value = expected_json
        mock_df.toJSON.return_value = mock_json_df
        mock_spark_read_json.return_value = mock_df
        main_inventory_prefix = "inventory-bucket/inventory-prefix"
        manifest_location = (
            f"s3://{main_inventory_prefix}/2026-06-07T01-00Z/manifest.json"
        )
        response = self.job._get_inventory_list(
            manifest_location=manifest_location
        )
        expected_response = [
            (
                f"s3a://{main_inventory_prefix}/data/"
                f"4af35350-4b46-4fa0-bc20-2bb1d4a93b4e.csv.gz"
            ),
            (
                f"s3a://{main_inventory_prefix}/data/"
                f"db376679-71ec-4d68-a919-4cae625d636e.csv.gz"
            ),
        ]
        self.assertEqual(response, expected_response)

    def test_get_inventory_df(self):

        docdb_info = self.example_docdb_info
        joined_df = self.job._get_inventory_df(
            s3_paths=[str(self.csv_file_location)],
            docdb_records=docdb_info,
            report_date="2026-06-07T01-00Z",
        )
        df_schema = joined_df.schema
        expected_output_df = (
            self.spark.read.option("multiLine", True)
            .schema(df_schema)
            .json(str(self.expected_output_df_file))
        )
        assertDataFrameEqual(joined_df, expected_output_df)

    @patch("pyspark.sql.DataFrameWriter.parquet")
    def test_write_df(self, mock_write_parquet: MagicMock):
        test_df = self.spark.createDataFrame([(1, "foo")], ["id", "value"])
        self.job._write_df(test_df)
        mock_write_parquet.assert_called_once_with("test_output_path")

    @patch(f"{CLS_REF}._get_docdb_info")
    @patch(f"{CLS_REF}._get_latest_manifest")
    @patch(f"{CLS_REF}._get_inventory_list")
    @patch(f"{CLS_REF}._get_inventory_df")
    @patch(f"{CLS_REF}._write_df")
    def test_run_job(
        self,
        mock_write_df: MagicMock,
        mock_get_inventory_df: MagicMock,
        mock_get_inventory_list: MagicMock,
        mock_get_latest_manifest: MagicMock,
        mock_get_docdb_info: MagicMock,
    ):
        mock_get_docdb_info.return_value = self.example_docdb_info
        latest_manifest_location = (
            "inventory-bucket/inventory-prefix/2026-06-07T01-00Z/manifest.json"
        )
        mock_get_latest_manifest.return_value = (
            latest_manifest_location,
            "2026-06-07T01-00Z",
        )
        mock_get_inventory_list.return_value = ["s3a://bucket/prefix/a.csv.gz"]
        mock_output_df = self.spark.read.option("multiLine", True).json(
            str(self.expected_output_df_file)
        )
        mock_get_inventory_df.return_value = mock_output_df
        with self.assertLogs(level="INFO") as captured:
            self.job.run_job()
        self.assertEqual(9, len(captured.output))
        mock_get_docdb_info.assert_called_once()
        mock_get_latest_manifest.assert_called_once()
        mock_get_inventory_list.assert_called_once_with(
            manifest_location=latest_manifest_location
        )
        mock_get_inventory_df.assert_called_once_with(
            s3_paths=["s3a://bucket/prefix/a.csv.gz"],
            docdb_records=self.example_docdb_info,
            report_date="2026-06-07T01-00Z",
        )
        mock_write_df.assert_called_once_with(mock_output_df)


if __name__ == "__main__":
    unittest.main()
