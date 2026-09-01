import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="events_daily",
    schema="events",
    catalog="csb_dev_analytics",
    comment="One row per UTC date with validated event volume",
    table_properties={"owner": "analytics", "cost_center": "eng", "dataset_name": "events"},
)
def events_daily():
    return (
        dlt.read("clean_events")
        .withColumn("date", F.to_date("ingested_at"))
        .groupBy("date")
        .agg(F.count("*").alias("event_count"))
    )
