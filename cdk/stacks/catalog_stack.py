from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class CatalogStack(Stack):

    def __init__(
        self,
        scope: Construct,
        id: str,
        data_bucket: s3.Bucket,
        stage: str,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        # One Glue database per layer — keeps Athena queries clean
        for layer in ["bronze", "silver", "gold"]:
            glue.CfnDatabase(
                self, f"{layer.capitalize()}Database",
                catalog_id=self.account,
                database_input=glue.CfnDatabase.DatabaseInputProperty(
                    name=f"clinical_{layer}_{stage}",
                    description=f"Clinical pipeline {layer} layer — {stage}",
                ),
            )

        # IAM role shared by all crawlers in this catalog
        crawler_role = iam.Role(
            self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        data_bucket.grant_read(crawler_role)

        # One crawler per layer — run after each pipeline execution to
        # register new partitions in the catalog so Athena can see them
        for layer in ["bronze", "silver", "gold"]:
            glue.CfnCrawler(
                self, f"{layer.capitalize()}Crawler",
                name=f"clinical-{layer}-crawler-{stage}",
                role=crawler_role.role_arn,
                database_name=f"clinical_{layer}_{stage}",
                targets=glue.CfnCrawler.TargetsProperty(
                    s3_targets=[
                        glue.CfnCrawler.S3TargetProperty(
                            path=f"s3://{data_bucket.bucket_name}/{layer}/",
                        )
                    ]
                ),
                schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                    update_behavior="UPDATE_IN_DATABASE",
                    # LOG instead of DELETE_FROM_DATABASE: if a schema change causes
                    # Glue to detect a new partition structure, you don't want it
                    # quietly dropping existing table definitions. Log it, investigate,
                    # decide deliberately.
                    delete_behavior="LOG",
                ),
            )
