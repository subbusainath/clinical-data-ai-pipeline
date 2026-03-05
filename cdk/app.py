import aws_cdk as cdk
from stacks.storage_stack import StorageStack
from stacks.catalog_stack import CatalogStack
from stacks.ingestion_stack import IngestionStack

app = cdk.App()

env   = cdk.Environment(account="YOUR_ACCOUNT_ID", region="us-east-1")
stage = app.node.try_get_context("stage") or "dev"

storage = StorageStack(app, f"ClinicalStorage-{stage}", stage=stage, env=env)

catalog = CatalogStack(
    app, f"ClinicalCatalog-{stage}",
    data_bucket=storage.data_bucket,
    stage=stage,
    env=env,
)

catalog.add_dependency(storage)

ingestion = IngestionStack(
    app, f"ClinicalIngestion-{stage}",
    data_bucket=storage.data_bucket,
    stage=stage,
    env=env,
)

ingestion.add_dependency(catalog)

app.synth()
