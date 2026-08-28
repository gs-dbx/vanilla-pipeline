import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="clean_events",
    schema="silver",
    comment="Parsed and validated events",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
def clean_events():
    return (
        dlt.read("raw_events")
        .withColumn("payload_parsed", F.from_json(F.col("payload"), "MAP<STRING,STRING>"))
        .drop("payload")
    )


@dlt.table(
    name="clean_users",
    schema="silver",
    comment="Deduplicated and validated users",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
def clean_users():
    return (
        dlt.read("raw_events")
        .dropDuplicates(["event_id"])
        .filter(F.col("event_id").isNotNull())
    )
