from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
)
from constructs import Construct


class StorageStack(Stack):

    def __init__(self, scope: Construct, id: str, stage: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.data_bucket = s3.Bucket(
            self, "ClinicalDataBucket",
            bucket_name=f"clinical-pipeline-{stage}-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                # Bronze: raw source data — archive to Glacier after 90 days
                s3.LifecycleRule(
                    id="BronzeLifecycle",
                    prefix="bronze/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        )
                    ],
                ),
                # Silver: validated data — archive to Glacier after 1 year
                s3.LifecycleRule(
                    id="SilverLifecycle",
                    prefix="silver/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(365),
                        )
                    ],
                ),
            ],
        )
