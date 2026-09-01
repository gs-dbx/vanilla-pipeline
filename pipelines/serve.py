import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="events_daily",
    schema="events",
    catalog="csb_dev_analytics",
    comment="Daily event rollup for BI",
    table_properties={"owner": "analytics", "cost_center": "eng", "dataset_name": "events"},
)
def events_daily():
    return (
        dlt.read("clean_events")
        .withColumn("date", F.to_date("ingested_at"))
        .groupBy("date")
        .agg(F.count("*").alias("event_count"))
    )


@dlt.table(
    name="debug_shortcut",
    schema="events",
    catalog="csb_dev_analytics",
    table_properties={"owner": "analytics", "cost_center": "eng"},
)
def debug_shortcut():
    return dlt.read("raw_events")   # bronze → gold: illegal


@dlt.table(
    name="debug_shortcut",
    schema="events",
    catalog="csb_dev_apps",
    table_properties={"owner": "analytics", "cost_center": "eng"},
)
def debug_shortcut():
    return dlt.read("raw_events")   # bronze → gold: illegal


@dlt.table(
    name="debug_shortcut",
    schema="events",
    catalog="csb_dev_analytics",
    table_properties={"owner": "analytics", "cost_center": "eng"},
)
def debug_shortcut():
    return dlt.read("raw_events")   # bronze → gold: illegal


@dlt.table(
    name="debug_shortcut",
    schema="events",
    catalog="csb_dev_apps",
    table_properties={"owner": "analytics", "cost_center": "eng"},
)
def debug_shortcut():
    return dlt.read("raw_events")   # bronze → gold: illegal
# arch-guard demo
