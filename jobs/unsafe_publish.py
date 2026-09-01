from pyspark.sql import SparkSession


spark = SparkSession.getActiveSession()

# Deliberately non-idempotent under task retry and uses an unsanctioned catalog.
events = spark.table("csb_dev_events_cleansed.events.clean_events")
events.write.mode("append").saveAsTable("legacy_warehouse.events.event_exports")

# Deliberately hides failure, so the job can report success after a broken publish.
try:
    spark.sql("DELETE FROM csb_dev_analytics.events.events_daily")
except Exception:
    pass
