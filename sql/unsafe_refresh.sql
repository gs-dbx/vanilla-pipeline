-- Deliberately destructive and unbounded for general SQL review coverage.
DELETE FROM csb_dev_analytics.events.events_daily;

-- Deliberately targets an unsanctioned catalog.
INSERT INTO legacy_warehouse.events.events_daily
SELECT * FROM csb_dev_events_cleansed.events.clean_events;
