import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Job initialisation ────────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_bucket",
        "dest_bucket",
        "icd10_reference_path",
        "processing_date",
    ],
)
# processing_date and icd10_reference_path are provided via CDK default_arguments
# and can be overridden per-run through Glue workflow run properties.

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_bucket        = args["source_bucket"]
dest_bucket          = args["dest_bucket"]
icd10_reference_path = args["icd10_reference_path"]
processing_date      = args["processing_date"]

bronze_base = f"s3://{source_bucket}/bronze"
silver_path = f"s3://{dest_bucket}/silver/processing_date={processing_date}"

# ── Load Bronze ───────────────────────────────────────────────────────────────

encounters = spark.read.parquet(f"{bronze_base}/encounters/")
conditions = spark.read.parquet(f"{bronze_base}/conditions/")
procedures = spark.read.parquet(f"{bronze_base}/procedures/")
patients   = spark.read.parquet(f"{bronze_base}/patients/")
notes      = spark.read.parquet(f"{bronze_base}/notes/")

# ── Load ICD-10 2024-CM reference ─────────────────────────────────────────────

icd10_ref = spark.read.option("header", True).csv(icd10_reference_path)

# Broadcast the valid code set to all workers. Without broadcast, every
# partition would trigger a full read of the reference CSV from S3.
valid_codes_set = set(row["code"] for row in icd10_ref.select("code").collect())
valid_codes_bc  = sc.broadcast(valid_codes_set)

# ── Step 1: Required field validation ─────────────────────────────────────────

encounters_valid = encounters.filter(
    F.col("Id").isNotNull()
    & F.col("PATIENT").isNotNull()
    & F.col("START").isNotNull()
)
dropped_count = encounters.count() - encounters_valid.count()
print(f"[Silver] Dropped {dropped_count:,} encounters missing required fields")

# ── Step 2: ICD-10 code validation ────────────────────────────────────────────

@F.udf("boolean")
def is_valid_icd10(code: str) -> bool:
    """
    Checks code against the 2024-CM reference set.
    Normalises dot notation first: E11.9 and E119 both map to the same lookup.
    Synthea and real EHR exports are inconsistent about dots — normalise both.
    """
    if code is None:
        return False
    normalised = code.replace(".", "").strip().upper()
    return normalised in valid_codes_bc.value

conditions_valid   = conditions.filter(is_valid_icd10(F.col("CODE")))
invalid_code_count = conditions.count() - conditions_valid.count()
print(f"[Silver] Flagged {invalid_code_count:,} conditions with invalid/deprecated codes")

# ── Step 3: Deduplication ──────────────────────────────────────────────────────
#
# The same encounter_id can appear across multiple Bronze ingestion batches when
# source systems re-export overlapping date ranges. Keep the most recently
# ingested version of each encounter.

dedup_window = Window.partitionBy("Id").orderBy(F.col("_ingestion_timestamp").desc())

encounters_deduped = (
    encounters_valid
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
dedup_count = encounters_valid.count() - encounters_deduped.count()
print(f"[Silver] Removed {dedup_count:,} duplicate encounter records")

# ── Step 4: Note normalisation ─────────────────────────────────────────────────

BOILERPLATE_PATTERNS = [
    r"(?i)this document is confidential.*",
    r"(?i)electronically signed by.*",
    r"(?i)printed on \d{1,2}/\d{1,2}/\d{4}.*",
    r"(?i)\[auto-generated\].*",
]


def _normalise_note(text: str | None) -> str | None:
    if text is None:
        return None
    text = text.lower().strip()
    for pattern in BOILERPLATE_PATTERNS:
        text = re.sub(pattern, "", text)
    return re.sub(r"\s+", " ", text).strip() or None


normalise_note_udf = F.udf(_normalise_note, "string")
notes_normalised = notes.withColumn("note_text", normalise_note_udf(F.col("note_text")))

# ── Step 5: Write Silver ───────────────────────────────────────────────────────
#
# Patients are written here too — Gold reads demographics from Silver, not Bronze.
# This keeps the Gold job's reads within a single Silver partition rather than
# crossing layers, which simplifies lineage and avoids schema drift surprises.

encounters_deduped.write.mode("overwrite").parquet(f"{silver_path}/encounters/")
conditions_valid.write.mode("overwrite").parquet(f"{silver_path}/conditions/")
procedures.write.mode("overwrite").parquet(f"{silver_path}/procedures/")
patients.write.mode("overwrite").parquet(f"{silver_path}/patients/")
notes_normalised.write.mode("overwrite").parquet(f"{silver_path}/notes/")

print(f"[Silver] Pipeline complete → {silver_path}")
job.commit()
