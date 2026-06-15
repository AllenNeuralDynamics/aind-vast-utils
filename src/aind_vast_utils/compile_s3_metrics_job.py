"""
Module for compiling metrics about our S3 buckets.
"""

import json
import logging
import os
import re
import sys
from typing import Dict, List, Tuple

import boto3
import pyspark.sql.functions as F
from aind_data_access_api.document_db import MetadataDbClient
from aind_settings_utils.aws import SecretsManagerBaseSettings
from pydantic import Field
from pydantic_settings import SettingsConfigDict
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)
level = os.getenv("LOG_LEVEL", logging.INFO)
logger.setLevel(level)

# Current schema of the csv files in the S3 Inventory report
CSV_SCHEMA = StructType(
    [
        StructField("Bucket", StringType(), True),
        StructField("Key", StringType(), True),
        StructField("VersionId", StringType(), True),
        StructField("IsLatest", BooleanType(), True),
        StructField("IsDeleteMarker", BooleanType(), True),
        StructField("Size", IntegerType(), True),
        StructField("LastModifiedDate", TimestampType(), True),
        StructField("ETag", StringType(), True),
        StructField("StorageClass", StringType(), True),
        StructField("IntelligentTieringAccessTier", StringType(), True),
    ]
)


class JobSettings(
    SecretsManagerBaseSettings,
    cli_parse_args=True,
    cli_ignore_unknown_args=True,
):
    """Settings needed to run CompileS3MetricsJob"""

    # noinspection SpellCheckingInspection
    model_config = SettingsConfigDict(env_prefix="CompileS3MetricsJob_")
    s3_inventory_bucket: str = Field(
        ...,
        title="S3 Inventory Bucket",
        description="Bucket where the inventory manifest is located.",
    )
    s3_inventory_prefix: str = Field(
        ...,
        title="S3 Inventory Prefix",
        description="Prefix where the s3 inventory manifest is located.",
    )
    bucket: str = Field(
        ...,
        title="S3 Bucket",
        description="The bucket that is being analyzed for it's metrics.",
    )
    output_location: str = Field(
        ...,
        title="Output Location",
        description="Output location for writing dataframe",
    )
    docdb_host: str = Field(
        ...,
        title="DocDB Host",
        description="Host for data asset metadata index",
    )
    spark_configs: Dict[str, str] = Field(
        {
            "spark.app.name": "S3InventoryMetrics",
            "spark.jars.packages": (
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
            ),
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            ),
        }
    )


class CompileS3MetricsJob:
    """
    Job to compile metrics about S3 metrics and generate a report.
    """

    def __init__(self, job_settings: JobSettings, spark: SparkSession):
        """Class constructor."""
        self.job_settings = job_settings
        self.spark = spark

    def _get_docdb_info(self) -> List[Tuple[str, str]]:
        """
        Get s3 location and project name from DocDB. This information is
        probably small enough to fit into memory.

        Returns
        -------
        List[Tuple[str, str]]
          A list of (prefix, project_name) tuples.

        """
        bucket = self.job_settings.bucket
        docdb_api_client = MetadataDbClient(
            host=self.job_settings.docdb_host,
        )
        filter_query = {"location": {"$regex": f"^s3://{bucket}"}}
        projection = {"location": 1, "data_description.project_name": 1}
        records = docdb_api_client.retrieve_docdb_records(
            projection=projection, filter_query=filter_query
        )
        mapped_records = []
        for record in records:
            project_name = (
                None
                if record.get("data_description") is None
                else record["data_description"].get("project_name")
            )
            prefix = record["location"].split("/")[-1]
            mapped_records.append((prefix, project_name))
        return mapped_records

    def _get_latest_manifest(self) -> Tuple[str, str]:
        """
        Get latest manifest location.

        Returns
        -------
        Tuple[str, str]
          (location of manifest json file, report date)

        """
        bucket = self.job_settings.s3_inventory_bucket
        prefix = self.job_settings.s3_inventory_prefix.strip("/") + "/"
        regex_pattern = r".*(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}Z)"
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=bucket, Prefix=prefix, Delimiter="/"
        )
        final_prefix = None
        report_date = None
        for page in page_iterator:
            if "CommonPrefixes" in page:
                for prefix in page["CommonPrefixes"]:
                    # Probably don't need to do a comparison since the results
                    # returned are sorted. We just want the last matched item
                    matched = re.search(regex_pattern, prefix["Prefix"])
                    if matched:
                        final_prefix = matched.group(0)
                        report_date = matched.group(1)
        if final_prefix and report_date:
            return f"{bucket}/{final_prefix}/manifest.json", report_date
        else:
            raise FileNotFoundError(
                f"Manifest not found in {bucket} and {prefix}!"
            )

    def _get_inventory_list(self, manifest_location: str) -> List[str]:
        """
        AWS stores the inventory across multiple csv files. This method parses
        the manifest file to extract these locations as a list.

        Parameters
        ----------
        manifest_location : str
          Location of the manifest file in S3

        Returns
        -------
        List[str]
          A list of s3 uris for the csv files that need to be parsed.

        """
        bucket = self.job_settings.s3_inventory_bucket
        manifest = json.loads(
            self.spark.read.option("multiLine", "true")
            .json(f"s3a://{manifest_location}")
            .toJSON()
            .first()
        )
        object_keys = [row["key"] for row in manifest["files"]]
        s3_paths = [f"s3a://{bucket}/{obj_key}" for obj_key in object_keys]
        return s3_paths

    def _get_inventory_df(
        self,
        s3_paths: List[str],
        docdb_records: List[Tuple[str, str]],
        report_date: str,
    ) -> DataFrame:
        """
        Parses the csv files and joins information from DocDB. The DataFrame is
        lazily evaluated.
        Parameters
        ----------
        s3_paths : List[str]
        docdb_records : List[Tuple[str, str]]
        report_date : str

        Returns
        -------
        DataFrame
          Columns (
          bucket, prefix, subprefix, storage_class,
          intelligent_tiering_access_tier, size_in_bytes, project_name,
          report_date
          )

        """
        full_df = (
            self.spark.read.format("csv")
            .option("header", "false")
            .schema(CSV_SCHEMA)
            .load(s3_paths)
        )
        # noinspection PyCallingNonCallable
        filtered_df = (
            full_df.withColumn(
                "Prefix", F.split(full_df["Key"], "/").getItem(0)
            )
            .withColumn("Subpath", F.split(full_df["Key"], "/").getItem(1))
            .withColumn(
                "Subprefix",
                F.concat_ws("/", F.col("Prefix"), F.col("Subpath")),
            )
            .where(
                (F.col("IsLatest") == True)  # noqa: E712
                & (F.col("IsDeleteMarker") == False)  # noqa: E712
            )
            .select(
                F.col("Bucket"),
                F.col("Prefix"),
                F.col("Subprefix"),
                F.col("Size"),
                F.col("StorageClass"),
                F.col("IntelligentTieringAccessTier"),
            )
        )
        docdb_df = self.spark.createDataFrame(
            docdb_records, ("Prefix", "ProjectName")
        )
        grouped_df = filtered_df.groupBy(
            "Bucket",
            "Prefix",
            "Subprefix",
            "StorageClass",
            "IntelligentTieringAccessTier",
        ).sum("Size")
        joined_df = (
            grouped_df.join(docdb_df, "Prefix", "left").withColumn(
                "ReportDate", F.lit(report_date)
            )
        ).withColumnsRenamed(
            {
                "Bucket": "bucket",
                "Prefix": "prefix",
                "Subprefix": "subprefix",
                "StorageClass": "storage_class",
                "IntelligentTieringAccessTier": (
                    "intelligent_tiering_access_tier"
                ),
                "sum(Size)": "size_in_bytes",
                "ProjectName": "project_name",
                "ReportDate": "report_date",
            }
        )
        return joined_df

    def _write_df(self, df: DataFrame):
        """
        Write the dataframe. This may take some time to complete.
        Parameters
        ----------
        df : DataFrame

        """
        output_location = self.job_settings.output_location
        df.write.parquet(output_location)

    def run_job(self):
        """Compile the metrics and generate a report."""
        logger.info(
            "Getting project name information. This may take a minute."
        )
        docdb_records = self._get_docdb_info()
        logger.info(f"Found {len(docdb_records)} records.")
        logger.info("Getting latest manifest location.")
        latest_manifest, report_date = self._get_latest_manifest()
        logger.info(f"Manifest location: {latest_manifest}.")
        logger.info("Starting Spark Session.")
        logger.info("Getting inventory list.")
        s3_paths = self._get_inventory_list(manifest_location=latest_manifest)
        logger.info(f"Inventory located across {len(s3_paths)} files.")
        logger.info("Defining DataFrame strategy. This may take a minute.")
        df = self._get_inventory_df(
            s3_paths=s3_paths,
            docdb_records=docdb_records,
            report_date=report_date,
        )
        logger.info("Writing DataFrame. This may take a while.")
        self._write_df(df)


if __name__ == "__main__":
    if len(sys.argv[1:]) == 2 and sys.argv[1] == "--job-settings":
        main_job_settings = JobSettings.model_validate_json(sys.argv[2])
    else:
        # noinspection PyArgumentList
        main_job_settings = JobSettings()
    logger.info(
        f"Starting job with the following settings: {main_job_settings}"
    )
    logger.info("Starting Spark Session.")
    spark_conf = SparkConf().setAll(
        list(main_job_settings.spark_configs.items())
    )
    with SparkSession.builder.config(conf=spark_conf).getOrCreate() as sp:
        main_job = CompileS3MetricsJob(
            job_settings=main_job_settings, spark=sp
        )
        main_job.run_job()
    logger.info("Job finished!")
