import argparse
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ── Job initialisation ────────────────────────────────────────────────────────

# getResolvedOptions only handles required args. For optional args with defaults,
# parse sys.argv directly — mixing the two avoids the "missing required argument"
# error that getResolvedOptions throws if an undeclared flag is absent.
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "dest_bucket"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_bucket = args["source_bucket"]
dest_bucket   = args["dest_bucket"]

parser = argparse.ArgumentParser()
parser.add_argument("--ingestion_date", default=None)
known_args, _ = parser.parse_known_args()
ingestion_date = (
    known_args.ingestion_date
    or datetime.now(timezone.utc).strftime("%Y-%m-%d")
)

# ── Tables to ingest ──────────────────────────────────────────────────────────

TABLES = ["encounters", "conditions", "procedures", "patients", "notes"]

for table in TABLES:
    source_path = f"s3://{source_bucket}/drop-zone/{table}/"
    dest_path = (
        f"s3://{dest_bucket}/bronze/{table}/"
        f"ingestion_date={ingestion_date}/"
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # keep everything as string in Bronze;
        .csv(source_path)               # casting happens in Silver with explicit types
    )

    # The only modifications Bronze makes are two lineage columns.
    # F.current_timestamp() is a Spark Column expression — evaluated lazily per
    # partition on the worker, not a Python scalar fetched to the driver.
    # F.lit() wraps the path string into a Column without any Spark action.
    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_path", F.lit(source_path))
    )

    df.write.mode("overwrite").parquet(dest_path)
    print(f"[Bronze] {table}: {df.count():,} records written → {dest_path}")

job.commit()
