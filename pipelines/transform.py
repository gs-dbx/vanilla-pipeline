import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="clean_events",
    schema="events",
    catalog="csb_dev_events_cleansed",
    comment="Parsed and validated events",
    table_properties={"owner": "platform", "cost_center": "eng", "dataset_name": "events"},
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
    schema="events",
    catalog="csb_dev_wrongdomain_cleansed",
    comment="Deduplicated and validated users",
    table_properties={"owner": "platform", "cost_center": "eng", "dataset_name": "events"},
)
def clean_users():
    return (
        dlt.read("raw_events")
        .dropDuplicates(["event_id"])
        .filter(F.col("event_id").isNotNull())
    )
