import hashlib
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Job initialisation ────────────────────────────────────────────────────────

# train_ratio, val_ratio, seed are provided via CDK default_arguments so they
# are always present in sys.argv and safe to declare as required here.
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_bucket",
        "dest_bucket",
        "processing_date",
        "train_ratio",
        "val_ratio",
        "seed",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_bucket   = args["source_bucket"]
dest_bucket     = args["dest_bucket"]
processing_date = args["processing_date"]
train_ratio     = float(args["train_ratio"])
val_ratio       = float(args["val_ratio"])
# test_ratio is implicit: 1.0 - train_ratio - val_ratio (0.15 with defaults)
seed = args["seed"]   # logged for audit trail; not used in hash-based split

silver_path = f"s3://{source_bucket}/silver/processing_date={processing_date}"

# ── Load Silver ───────────────────────────────────────────────────────────────

encounters = spark.read.parquet(f"{silver_path}/encounters/")
conditions  = spark.read.parquet(f"{silver_path}/conditions/")
procedures  = spark.read.parquet(f"{silver_path}/procedures/")
patients    = spark.read.parquet(f"{silver_path}/patients/")
notes       = spark.read.parquet(f"{silver_path}/notes/")

# ── Compute patient age at time of encounter ──────────────────────────────────

encounters = (
    encounters
    .join(
        patients.select("Id", "BIRTHDATE"),
        encounters["PATIENT"] == patients["Id"],
        "left",
    )
    .withColumn(
        "patient_age",
        (F.datediff(
            F.col("START").cast("date"),
            F.col("BIRTHDATE").cast("date"),
        ) / 365).cast("int"),
    )
    .drop(patients["Id"])
    .drop("BIRTHDATE")
)

# ── Aggregate codes per encounter ─────────────────────────────────────────────
#
# Synthea writes one row per condition per encounter in conditions.csv.
# We aggregate into arrays so the Gold record is one row per encounter.

icd10_agg = (
    conditions
    .groupBy("ENCOUNTER")
    .agg(F.collect_set("CODE").alias("icd10_labels"))
)

cpt_agg = (
    procedures
    .groupBy("ENCOUNTER")
    .agg(F.collect_set("CODE").alias("cpt_labels"))
)

# ── Build the flat Gold record ────────────────────────────────────────────────
#
# ENCOUNTER_CLASS is the Synthea field for visit category (ambulatory, inpatient, etc.)
# ORGANIZATION is the provider organisation — both exist in Synthea's encounters.csv.
# There is no DEPARTMENT column in Synthea output.

gold = (
    encounters
    .join(icd10_agg, encounters["Id"] == icd10_agg["ENCOUNTER"], "left")
    .join(cpt_agg,   encounters["Id"] == cpt_agg["ENCOUNTER"],   "left")
    .join(notes,     encounters["Id"] == notes["encounter_id"],   "left")
    .select(
        encounters["Id"].alias("encounter_id"),
        encounters["PATIENT"].alias("patient_id"),
        F.col("patient_age"),
        encounters["ENCOUNTER_CLASS"].alias("visit_type"),
        encounters["ORGANIZATION"].alias("organization"),
        F.col("note_text").alias("clinical_note"),
        F.col("icd10_labels"),
        F.col("cpt_labels"),
    )
)

# Drop records without a clinical note — no text, nothing for the model to work with
gold = gold.filter(
    F.col("clinical_note").isNotNull()
    & (F.trim(F.col("clinical_note")) != "")
)

# ── Deterministic train / val / test split ─────────────────────────────────────
#
# Why hash-based rather than a random split with a fixed seed?
#
# Random splits are fragile under data growth. Add new records, re-run with the
# same seed, and the split boundaries shift — existing encounters can move between
# train and test. For a system that retrains periodically on growing clinical data,
# this makes evaluation metrics incomparable across model versions.
#
# An MD5 hash on encounter_id is fully deterministic: the same encounter always
# lands in the same split regardless of dataset size or run order. New encounters
# get assigned consistently on their first appearance.
#
# usedforsecurity=False: Python 3.11 raises ValueError for MD5 in FIPS-compliant
# environments without this flag. AWS GovCloud and many regulated healthcare
# environments enforce FIPS. This one flag prevents a silent runtime failure.

_train_bucket_max = int(train_ratio * 100)
_val_bucket_max   = int((train_ratio + val_ratio) * 100)


def _assign_split(encounter_id: str) -> str:
    bucket = (
        int(
            hashlib.md5(
                encounter_id.encode(),
                usedforsecurity=False,
            ).hexdigest(),
            16,
        )
        % 100
    )
    if bucket < _train_bucket_max:
        return "train"
    if bucket < _val_bucket_max:
        return "val"
    return "test"


assign_split_udf = F.udf(_assign_split, StringType())
gold = gold.withColumn("split", assign_split_udf(F.col("encounter_id")))

# Verify distribution before committing the write
print(f"[Gold] Split distribution (seed={seed}, train={train_ratio}, val={val_ratio}):")
gold.groupBy("split").count().orderBy("split").show()

# ── Write Gold ────────────────────────────────────────────────────────────────

gold_path = f"s3://{dest_bucket}/gold/version={processing_date}"

(
    gold.write
    .mode("overwrite")
    .partitionBy("split")
    .parquet(gold_path)
)

print(f"[Gold] {gold.count():,} records written → {gold_path}")
job.commit()
