import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="raw_events",
    schema="bronze",
    comment="Raw event stream from Kafka",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
def raw_events():
    return (
        dlt.read_stream("kafka_events")
        .select(
            F.col("key").cast("string").alias("event_id"),
            F.col("value").cast("string").alias("payload"),
            F.col("timestamp").alias("ingested_at"),
        )
    )


@dlt.table(
    name="raw_users",          # <-- naming violation: CamelCase, not snake_case
    schema="bronze",
    comment="Raw user records from S3 landing zone",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
def RawUsers():
    return (
        dlt.read_stream("s3_landing_zone")
        .select("user_id", "email", "created_at")
    )


@dlt.table(
    name="bad_shortcut",
    schema="gold",
    table_properties={"owner": "analytics", "cost_center": "eng"},
)
def bad_shortcut():
    return dlt.read("raw_events")   # bronze → gold: illegal    


@dlt.table(
    name="BadCatalogTable",
    schema="bronze",
    catalog="legacy_warehouse",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
def BadCatalogTable():
    return dlt.read_stream("kafka_events")
