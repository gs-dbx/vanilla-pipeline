import dlt
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


# No @dlt.expect on this bronze table — bad data propagates silently into silver
@dlt.table(
    name="raw_order_events",
    schema="bronze",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
def raw_order_events():
    # Hardcoded path — should be a pipeline parameter
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/landing/orders/schema")
        .load("/mnt/landing/orders/raw")
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("amount").cast("double"),
            F.col("status"),
            F.col("event_time").cast("timestamp"),
            # Doing heavy transformation in bronze — this is silver's job
            F.when(F.col("amount") > 1000, "high_value")
             .when(F.col("amount") > 100, "mid_value")
             .otherwise("low_value").alias("value_tier"),
            F.date_trunc("hour", F.col("event_time")).alias("event_hour"),
            F.md5(F.concat_ws("|", F.col("order_id"), F.col("customer_id"))).alias("dedup_key"),
        )
    )


# Missing comment= — table undiscoverable in Unity Catalog
# Also no @dlt.expect despite reading from an unvalidated bronze source
@dlt.table(
    name="raw_order_refunds",
    schema="bronze",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
def raw_order_refunds():
    # spark.read() instead of dlt.read() — breaks DLT lineage
    spark = SparkSession.getActiveSession()
    return (
        spark.read.format("delta")
        .load("/mnt/landing/refunds/delta")
        .select("order_id", "refund_amount", "refund_date")
    )


@dlt.table(
    name="clean_orders",
    schema="silver",
    comment="Validated and enriched order records",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def clean_orders():
    return (
        dlt.read_stream("raw_order_events")
        .filter(F.col("status") != "CANCELLED")
        .withColumn("processed_at", F.current_timestamp())
    )


# Gold table reading from a streaming silver source with no watermark — will cause
# issues when the pipeline switches to triggered/batch mode
@dlt.table(
    name="orders_hourly_summary",
    schema="gold",
    comment="Hourly order aggregation for BI",
    table_properties={"owner": "analytics", "cost_center": "eng"},
)
def orders_hourly_summary():
    return (
        dlt.read_stream("clean_orders")
        .groupBy("event_hour", "value_tier")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("total_amount"),
        )
    )
