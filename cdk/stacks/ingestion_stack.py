from aws_cdk import (
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sns as sns,
)
from constructs import Construct


class IngestionStack(Stack):

    def __init__(
        self,
        scope: Construct,
        id: str,
        data_bucket: s3.Bucket,
        stage: str,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        # ── IAM role shared by all Glue jobs ──────────────────────────────────

        glue_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        data_bucket.grant_read_write(glue_role)

        # ── SNS topic for pipeline failure alerts ──────────────────────────────

        alert_topic = sns.Topic(
            self, "PipelineAlerts",
            topic_name=f"clinical-pipeline-alerts-{stage}",
        )
        # Uncomment to subscribe an on-call email:
        # sns.Subscription(self, "AlertEmail",
        #     topic=alert_topic,
        #     protocol=sns.SubscriptionProtocol.EMAIL,
        #     endpoint="your-team@example.com",
        # )

        # ── Helper: create a Glue 5.0 ETL job ─────────────────────────────────

        def create_glue_job(
            job_id: str,
            script_key: str,
            description: str,
            extra_args: dict | None = None,
        ) -> tuple[glue.CfnJob, str]:
            """
            Returns (CfnJob construct, physical job name as a plain string).

            We return the physical name as a plain string — not job.name (a CDK token)
            — so that CfnTrigger predicates and actions can reference job names without
            hitting circular token resolution issues between conditional triggers.
            """
            physical_name = f"clinical-{job_id.lower()}-{stage}"

            default_args: dict = {
                "--source_bucket":                    data_bucket.bucket_name,
                "--dest_bucket":                      data_bucket.bucket_name,
                "--enable-metrics":                   "true",
                "--enable-continuous-cloudwatch-log": "true",
                # Job bookmarks track which S3 partitions have already been processed.
                # Without this, every run reprocesses ALL historical data from scratch.
                "--job-bookmark-option":              "job-bookmark-enable",
                "--TempDir": (
                    f"s3://{data_bucket.bucket_name}/tmp/{job_id.lower()}/"
                ),
            }
            if extra_args:
                default_args.update(extra_args)

            job = glue.CfnJob(
                self, job_id,
                name=physical_name,
                description=description,
                role=glue_role.role_arn,
                command=glue.CfnJob.JobCommandProperty(
                    name="glueetl",
                    python_version="3",          # "3" = Python 3.x; Glue 5.0 uses 3.11
                    script_location=(
                        f"s3://{data_bucket.bucket_name}/scripts/{script_key}"
                    ),
                ),
                glue_version="5.0",             # Spark 3.5 + Python 3.11 — current as of 2026
                worker_type="G.1X",             # 4 vCPU / 16 GB per worker
                number_of_workers=5,
                timeout=120,                    # minutes; increase for larger datasets
                default_arguments=default_args,
            )
            return job, physical_name

        bronze_job, bronze_name = create_glue_job(
            "BronzeIngestion",
            "bronze/ingest_to_bronze.py",
            "Ingest raw Synthea CSVs to Bronze layer",
        )
        silver_job, silver_name = create_glue_job(
            "SilverTransform",
            "silver/bronze_to_silver.py",
            "Validate and transform Bronze to Silver",
            extra_args={
                "--icd10_reference_path": (
                    f"s3://{data_bucket.bucket_name}/reference/icd10_2024.csv"
                ),
            },
        )
        gold_job, gold_name = create_glue_job(
            "GoldFeatures",
            "gold/silver_to_gold.py",
            "Build feature-ready Gold layer from Silver",
            extra_args={
                "--train_ratio": "0.70",
                "--val_ratio":   "0.15",
                "--seed":        "42",
            },
        )

        # ── Glue Workflow: Bronze → Silver → Gold ──────────────────────────────

        workflow_name = f"clinical-pipeline-{stage}"
        workflow = glue.CfnWorkflow(
            self, "ClinicalPipelineWorkflow",
            name=workflow_name,
            description="End-to-end clinical data pipeline",
        )

        # Trigger 1: ON_DEMAND — fires when the workflow is started manually or by a scheduler
        bronze_trigger = glue.CfnTrigger(
            self, "BronzeTrigger",
            name=f"trigger-bronze-{stage}",
            workflow_name=workflow_name,
            type="ON_DEMAND",
            actions=[glue.CfnTrigger.ActionProperty(job_name=bronze_name)],
        )
        bronze_trigger.add_dependency(workflow)
        bronze_trigger.add_dependency(bronze_job)

        # Trigger 2: Start Silver only when Bronze SUCCEEDED
        silver_trigger = glue.CfnTrigger(
            self, "SilverTrigger",
            name=f"trigger-silver-{stage}",
            workflow_name=workflow_name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        job_name=bronze_name,
                        state="SUCCEEDED",
                        logical_operator="EQUALS",
                    )
                ]
            ),
            actions=[glue.CfnTrigger.ActionProperty(job_name=silver_name)],
        )
        silver_trigger.add_dependency(workflow)
        silver_trigger.add_dependency(bronze_job)
        silver_trigger.add_dependency(silver_job)

        # Trigger 3: Start Gold only when Silver SUCCEEDED
        gold_trigger = glue.CfnTrigger(
            self, "GoldTrigger",
            name=f"trigger-gold-{stage}",
            workflow_name=workflow_name,
            type="CONDITIONAL",
            start_on_creation=True,
            predicate=glue.CfnTrigger.PredicateProperty(
                conditions=[
                    glue.CfnTrigger.ConditionProperty(
                        job_name=silver_name,
                        state="SUCCEEDED",
                        logical_operator="EQUALS",
                    )
                ]
            ),
            actions=[glue.CfnTrigger.ActionProperty(job_name=gold_name)],
        )
        gold_trigger.add_dependency(workflow)
        gold_trigger.add_dependency(silver_job)
        gold_trigger.add_dependency(gold_job)

        # ── EventBridge rules for failure alerting ─────────────────────────────
        #
        # Why EventBridge and not CloudWatch metric alarms?
        #
        # The most obvious choice is a CloudWatch alarm on
        # 'glue.driver.aggregate.numFailedTasks'. The problem: that metric counts
        # individual Spark task failures *inside* a running job. A job can FAIL
        # completely — driver OOM, script exception, timeout, permission error —
        # with exactly zero failed tasks recorded. The metric alarm would never
        # fire for the failure modes you actually care about most.
        #
        # EventBridge catches the actual Glue job state transition to
        # FAILED / ERROR / TIMEOUT. That's the signal you want.

        for job_construct, job_phys_name, label in [
            (bronze_job, bronze_name, "Bronze"),
            (silver_job, silver_name, "Silver"),
            (gold_job,   gold_name,   "Gold"),
        ]:
            rule = events.Rule(
                self, f"{label}FailureRule",
                rule_name=f"clinical-{label.lower()}-failure-{stage}",
                description=f"Alert when {label} Glue job enters a failed state",
                event_pattern=events.EventPattern(
                    source=["aws.glue"],
                    detail_type=["Glue Job State Change"],
                    detail={
                        "jobName": [job_phys_name],
                        "state":   ["FAILED", "ERROR", "TIMEOUT"],
                    },
                ),
            )
            rule.add_target(targets.SnsTopic(alert_topic))
