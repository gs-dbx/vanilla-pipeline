import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="raw_events",
    schema="events",
    catalog="csb_dev_events_stage",
    comment="Raw event stream from Kafka",
    table_properties={"owner": "platform", "cost_center": "eng", "dataset_name": "events"},
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
def raw_events():
    return dlt.read_stream("kafka_events").select(
        F.col("key").cast("string").alias("event_id"),
        F.col("value").cast("string").alias("payload"),
        F.col("timestamp").alias("ingested_at"),
    )


@dlt.table(
    name="raw_users",
    schema="events",
    catalog="csb_dev_events_stage",
    comment="Raw user records from the landing zone",
    table_properties={"owner": "platform", "cost_center": "eng", "dataset_name": "events"},
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def raw_users():
    return dlt.read_stream("s3_landing_zone").select("user_id", "email", "created_at")
