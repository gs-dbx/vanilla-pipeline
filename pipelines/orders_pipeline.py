import dlt
from pyspark.sql import functions as F
from pyspark.sql import SparkSession  # used below for spark.read — intentional lineage break demo


# No @dlt.expect on this bronze table — bad data propagates silently into silver.
# Also missing comment= so this table is undiscoverable in Unity Catalog.
@dlt.table(
    name="raw_order_events",
    schema="orders",
    catalog="csb_dev_orders_stage",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
def raw_order_events():
    return (
        dlt.read_stream("kafka_events")
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("amount").cast("double"),
            F.col("status"),
            F.col("event_time").cast("timestamp"),
            # Heavy business logic in bronze — classification belongs in silver
            F.when(F.col("amount") > 1000, "high_value")
             .when(F.col("amount") > 100, "mid_value")
             .otherwise("low_value").alias("value_tier"),
            F.date_trunc("hour", F.col("event_time")).alias("event_hour"),
            F.md5(F.concat_ws("|", F.col("order_id"), F.col("customer_id"))).alias("dedup_key"),
        )
    )


# Missing comment= — table undiscoverable in Unity Catalog.
# No @dlt.expect — null refund amounts will corrupt silver aggregates silently.
# spark.read() instead of dlt.read() breaks DLT lineage graph.
@dlt.table(
    name="raw_order_refunds",
    schema="orders",
    catalog="csb_dev_orders_stage",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
def raw_order_refunds():
    spark = SparkSession.getActiveSession()
    return (
        spark.read.table("legacy_warehouse.orders.external_refunds")
        .select("order_id", "refund_amount", "refund_date")
    )


@dlt.table(
    name="clean_orders",
    schema="orders",
    catalog="csb_dev_orders_cleansed",
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
    schema="orders",
    catalog="csb_dev_analytics",
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
