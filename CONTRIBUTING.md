# Contributing pipeline code

Every pull request that changes pipeline code is reviewed against `arch-contract.yaml`. Follow the rules below before pushing; deterministic errors are currently advisory in this demonstration repository but are designed to become blocking under enforcement.

## Contract rules

### Naming

Table logical names must match `^[a-z][a-z0-9_]+$`: begin with a lowercase letter and then use lowercase letters, numbers, and underscores. Bronze table names must additionally start with `raw_`.

```python
@dlt.table(
    name="raw_order_events",
    schema="bronze",
    comment="Raw order events from the commerce event stream",
    table_properties={"owner": "commerce", "cost_center": "sales"},
)
def raw_order_events():
    return dlt.read_stream("kafka_events")
```

The decorator's `name=` is the logical name; without it, the Python function name is used. Keep both compliant and identical for readability. DAB pipeline display names must match `^plp_(bronze|silver|gold)_[a-z0-9_]+$`, and job display names should match `^job_[a-z0-9_]+$`:

```yaml
resources:
  pipelines:
    plp_silver_orders_transform:
      name: plp_silver_orders_transform
      catalog: dev_analytics
      target: silver
  jobs:
    job_refresh_orders:
      name: job_refresh_orders
```

### Catalogs

Literal catalog references may only use:

- `dev_analytics`
- `staging_analytics`
- `prod_analytics`

This applies to DLT decorator `catalog=`, DAB pipeline `catalog`, three-part Spark/SQL reads, Spark writes, and catalog-like DAB task parameters.

```python
orders = spark.table("dev_analytics.silver.clean_orders")
orders.write.mode("overwrite").saveAsTable("dev_analytics.gold.orders_daily")
```

Do not use personal, legacy, or newly provisioned catalogs until the contract change is reviewed and merged.

### Medallion flow

The permitted flow is external → bronze → silver → gold:

- Bronze reads declared external sources and writes bronze.
- Silver reads bronze and writes silver.
- Gold reads silver and writes gold.

```python
@dlt.table(name="clean_orders", schema="silver", comment="Validated orders")
def clean_orders():
    return dlt.read("raw_orders")


@dlt.table(name="orders_daily", schema="gold", comment="Daily validated order totals")
def orders_daily():
    return dlt.read("clean_orders").groupBy("order_date").count()
```

Never make gold read `raw_orders` directly. arch-guard infers tiers from literal `schema=` values and bronze/silver/gold name prefixes, and checks literal `dlt.read()` and `dlt.read_stream()` sources.

## DLT review expectations

The LLM reviewer adds non-blocking `[LLM]` warnings for important practices that are not deterministic contract rules.

### Put quality expectations on bronze tables

Declare the minimum validity rule for each ingestion asset. Choose behavior deliberately:

```python
@dlt.expect("has_event_id", "event_id IS NOT NULL")
```

retains invalid rows and records metrics, while:

```python
@dlt.expect_or_drop("has_event_id", "event_id IS NOT NULL")
```

drops invalid rows. Use a quarantine or failure policy where loss is unacceptable; do not add a meaningless always-true expectation merely to silence review.

### Add `comment=` to every table

The comment becomes discoverable metadata in Unity Catalog. Describe purpose, grain, and important semantics:

```python
@dlt.table(
    name="orders_daily",
    schema="gold",
    comment="One row per UTC order date with validated order count and gross value",
)
```

### Preserve DLT lineage

Inside a DLT table function, read declared pipeline assets with `dlt.read()` or `dlt.read_stream()`:

```python
def clean_orders():
    return dlt.read("raw_orders").filter("order_id IS NOT NULL")
```

Do not bypass the graph with:

```python
def clean_orders():
    return spark.read.table("dev_analytics.bronze.raw_orders")
```

The latter hides the intended DLT dependency and can impair lineage and update planning. Also avoid hard-coded storage paths and connection strings; supply them through pipeline configuration. Keep ingestion tables light, move business transformations into silver, document gold's serving consumer, and review stream-to-batch gold designs for watermark/state risks.

## Test before pushing

Clone the engine beside this repository and install its dependencies once:

```bash
cd /home/greg.skinner/gs-dbx/arch-guard
python3 -m pip install -r requirements.txt
cd /home/greg.skinner/gs-dbx/vanilla-pipeline
```

Check every tracked file:

```bash
./run_check.sh
```

Check only your branch diff:

```bash
git fetch origin main
./run_check.sh origin/main HEAD
```

Review both stdout and `summary.md`. `findings.sarif` contains the machine-readable equivalent. Local FM review runs only when you provide a reachable `FM_ENDPOINT` and Databricks SDK credentials; deterministic checks require no workspace.

## Open and interpret the PR

Push the branch and open a PR against `main`. The workflow runs when the diff touches `pipelines/**`, `jobs/**`, `notebooks/**`, any `.sql`, `databricks.yml`, or `arch-contract.yaml`.

Open the PR check named **arch-guard**, then open its Actions job summary. For each row:

1. Use **File:Line** to find the asset.
2. Use **Source** to distinguish deterministic `Linter` output from advisory `LLM` output.
3. Treat `error` as a contract breach even while the repository is in advisory mode.
4. Fix the code and push; the workflow reruns automatically.
5. If a result is intentionally accepted, add a reviewed waiver rather than changing unrelated rules.

LLM findings always start `[LLM]` and are capped at warning/note. A missing LLM result does not mean the FM call succeeded; check logs for `arch-guard [fm]: API call failed — ...`. Linter results are still authoritative.

## Fix or waive?

Fix the code when the contract reflects the intended architecture, including ordinary naming, wrong catalogs, bronze-to-gold shortcuts, absent metadata, and missing quality controls.

Use `.arch-waivers.yaml` only when all of these are true:

- the finding and risk have been understood;
- the deviation is a false positive, an unavoidable compatibility constraint, or a time-bounded migration;
- the reason names the business/technical constraint and tracking work;
- an appropriate reviewer approves it;
- an expiration is supplied for temporary deviations.

```yaml
waivers:
  - rule_id: medallion.illegal_read
    file: pipelines/serve.py
    line: 26
    reason: >-
      DATA-4890 tracks migration of the legacy debug consumer to clean_events;
      preserving its bronze shape is required until the 2026-10-15 cutover.
    approved_by: data-platform-admins
    expires: 2026-10-15
```

The `rule_id` and file must exactly match the summary. `line` is optional; omitting it waives all occurrences of that rule in the file. After expiration, the finding automatically becomes active. A waiver is code and must be reviewed in the PR.

Do not waive a finding merely to make the table shorter or because advisory mode permits merging. Do not edit `arch-contract.yaml` to legalize a one-off exception.

## Add an external source or catalog

Architecture expansion requires a contract PR and corresponding Databricks provisioning; YAML alone grants no network or data permission.

To declare an external source, append it under `external_sources` with a stable logical name and type:

```yaml
external_sources:
  - name: kafka_events
    type: kafka
  - name: s3_landing_zone
    type: cloud_storage
  - name: crm_change_feed
    type: kafka
```

Use that logical name in bronze code:

```python
@dlt.table(
    name="raw_crm_changes",
    schema="bronze",
    comment="Raw CRM change events",
    table_properties={"owner": "crm", "cost_center": "sales"},
)
@dlt.expect("has_record_id", "record_id IS NOT NULL")
def raw_crm_changes():
    return dlt.read_stream("crm_change_feed")
```

The current deterministic engine does not yet enforce `external_sources`; it is still the reviewed declaration and may ground LLM/future checks.

To authorize a catalog, append a schema-valid entry:

```yaml
sanctioned_catalogs:
  - name: dev_analytics
    env: dev
  - name: staging_analytics
    env: staging
  - name: prod_analytics
    env: prod
  - name: research_analytics
    env: dev
```

Catalog names must match `^[a-z][a-z0-9_]+$`, and `env` must be `dev`, `staging`, or `prod`. In the same change, document ownership, provision schemas and privileges, update the relevant DAB target, and obtain platform review. Because changing `arch-contract.yaml` makes arch-guard rescan all tracked pipeline files, resolve every newly surfaced finding before merging.
