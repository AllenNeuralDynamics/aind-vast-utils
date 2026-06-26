"""
Module for compiling metrics about our S3 buckets.
"""

import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Literal, Optional, Tuple

import boto3
import pyspark.sql.functions as F
from aind_data_access_api.document_db import MetadataDbClient
from aind_settings_utils.aws import SecretsManagerBaseSettings
from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import SettingsConfigDict
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)
level = os.getenv("LOG_LEVEL", logging.INFO)
logger.setLevel(level)


class OutputTarget(BaseModel):
    """OutputTarget model."""

    output_type: Literal["parquet", "postgres"] = Field("parquet")
    table_name: str = Field("weekly_report")
    output_location: Optional[str] = Field(None)
    db_username: Optional[str] = Field(None)
    db_password: Optional[SecretStr] = Field(None)
    db_url: Optional[str] = Field(None)
    db_save_mode: Literal["overwrite", "append", "ignore", "errorifexists"] = (
        Field(default="overwrite")
    )

    @model_validator(mode="after")
    def check_output_type_requirements(self) -> "OutputTarget":
        """Check fields are not None depending on format."""
        if self.output_type == "parquet" and self.output_location is None:
            raise ValueError(
                "output_location must be specified for parquet output_type!"
            )
        elif self.output_type == "postgres" and any(
            x is None
            for x in (self.db_username, self.db_password, self.db_url)
        ):
            raise ValueError(
                "db settings must be specified for postgres output_type!"
            )
        return self


class JobSettings(
    SecretsManagerBaseSettings,
    cli_parse_args=True,
    cli_ignore_unknown_args=True,
):
    """Settings needed to run CompileS3MetricsJob"""

    # noinspection SpellCheckingInspection
    model_config = SettingsConfigDict(env_prefix="CompileS3MetricsJob_")
    inventory_format: Literal["csv", "parquet"] = Field(
        default="parquet",
        title="Inventory Format",
        description="File format the inventory is stored under",
    )
    inventory_schema: Dict[str, Any] = Field(
        default={
            "fields": [
                {
                    "metadata": {},
                    "name": "bucket",
                    "nullable": True,
                    "type": "string",
                },
                {
                    "metadata": {},
                    "name": "key",
                    "nullable": True,
                    "type": "string",
                },
                {
                    "metadata": {},
                    "name": "version_id",
                    "nullable": True,
                    "type": "string",
                },
                {
                    "metadata": {},
                    "name": "is_latest",
                    "nullable": True,
                    "type": "boolean",
                },
                {
                    "metadata": {},
                    "name": "is_delete_marker",
                    "nullable": True,
                    "type": "boolean",
                },
                {
                    "metadata": {},
                    "name": "size",
                    "nullable": True,
                    "type": "long",
                },
                {
                    "metadata": {},
                    "name": "last_modified_date",
                    "nullable": True,
                    "type": "timestamp",
                },
                {
                    "metadata": {},
                    "name": "e_tag",
                    "nullable": True,
                    "type": "string",
                },
                {
                    "metadata": {},
                    "name": "storage_class",
                    "nullable": True,
                    "type": "string",
                },
                {
                    "metadata": {},
                    "name": "intelligent_tiering_access_tier",
                    "nullable": True,
                    "type": "string",
                },
            ],
            "type": "struct",
        },
        title="Inventory Schema",
        description="Schema of the inventory files",
    )
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
    output_target: Optional[OutputTarget] = Field(
        None,
        title="Output Target",
        description="Output target for writing dataframe",
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
                "org.apache.hadoop:hadoop-aws:3.4.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            ),
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "com.amazonaws.auth.profile.ProfileCredentialsProvider"
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

    def _get_inventory_df(self, s3_paths: List[str]) -> DataFrame:
        """
        Get the inventory DataFrame from S3.

        Parameters
        ----------
        s3_paths : List[str]
          S3 paths of the files to be parsed.

        Returns
        -------
        DataFrame

        """
        inventory_schema = StructType.fromJson(
            self.job_settings.inventory_schema
        )
        full_df = (
            self.spark.read.format(self.job_settings.inventory_format)
            .option("header", "false")
            .option("inferSchema", "false")
            .option("mode", "FAILFAST")
            .schema(inventory_schema)
            .load(s3_paths)
        )
        return full_df

    def _transform_inventory_df(
        self,
        inventory_df: DataFrame,
        docdb_records: List[Tuple[str, str]],
        report_date: str,
    ) -> DataFrame:
        """
        Parses the csv files and joins information from DocDB. The DataFrame is
        lazily evaluated.
        Parameters
        ----------
        inventory_df : DataFrame
        docdb_records : List[Tuple[str, str]]
        report_date : str

        Returns
        -------
        DataFrame
          Columns (
          bucket, prefix, subprefix, storage_class,
          intelligent_tiering_access_tier, size_in_bytes, number_of_files,
          project_name, report_date
          )

        """
        # noinspection PyCallingNonCallable
        filtered_df = (
            inventory_df.withColumn(
                "split_key", F.split(inventory_df["key"], "/")
            )
            .withColumn("prefix", F.split(inventory_df["key"], "/").getItem(0))
            .withColumn(
                "subprefix",
                F.when(
                    F.size(F.split(inventory_df["key"], "/")) >= 2,
                    F.concat_ws(
                        "/",
                        F.col("prefix"),
                        F.split(inventory_df["key"], "/").getItem(1),
                    ),
                ).otherwise(None),
            )
            .where(
                (F.col("is_latest") == True)  # noqa: E712
                & (F.col("is_delete_marker") == False)  # noqa: E712
            )
            .select(
                F.col("bucket"),
                F.col("prefix"),
                F.col("subprefix"),
                F.col("size"),
                F.col("storage_class"),
                F.col("intelligent_tiering_access_tier"),
            )
        )
        docdb_df = self.spark.createDataFrame(
            data=docdb_records,
            schema=StructType(
                [
                    StructField("prefix", StringType(), False),
                    StructField("project_name", StringType(), True),
                ]
            ),
        )
        grouped_df = filtered_df.groupBy(
            "bucket",
            "prefix",
            "subprefix",
            "storage_class",
            "intelligent_tiering_access_tier",
        ).agg(
            F.sum("size").alias("size_in_bytes"),
            F.count("size").alias("number_of_files"),
        )
        joined_df = grouped_df.join(docdb_df, "prefix", "left").withColumn(
            "report_date", F.lit(report_date)
        )
        return joined_df

    def _write_df(self, df: DataFrame):
        """
        Write the dataframe. This may take some time to complete.
        Parameters
        ----------
        df : DataFrame

        """
        output_target = self.job_settings.output_target
        if output_target is None:
            logger.info("No target set. Logging first few rows.")
            for row in df.limit(10).toLocalIterator():
                logger.info(f"{row}")
        elif output_target.output_type == "parquet":
            logger.info("Writing to local parquet files.")
            output_location = os.path.join(
                output_target.output_location, output_target.table_name
            )
            df.write.parquet(output_location)
        else:
            logger.info("Writing to postgres database.")
            properties = {
                "user": output_target.db_username,
                "password": output_target.db_password.get_secret_value(),
                "driver": "org.postgresql.Driver",
                "batchsize": "5000",
                "stringtype": "unspecified",
            }
            df.repartition(numPartitions=4).write.jdbc(
                url=output_target.db_url,
                table=output_target.table_name,
                mode=output_target.db_save_mode,
                properties=properties,
            )

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
        inventory_df = self._get_inventory_df(s3_paths=s3_paths)
        df = self._transform_inventory_df(
            inventory_df=inventory_df,
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
    sp = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    try:
        main_job = CompileS3MetricsJob(
            job_settings=main_job_settings, spark=sp
        )
        main_job.run_job()
    finally:
        sp.stop()
    logger.info("Job finished!")
