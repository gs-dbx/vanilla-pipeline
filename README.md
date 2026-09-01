# Vanilla Databricks pipeline

This repository is a reference Databricks DLT/Lakeflow Declarative Pipeline organized as bronze, silver, and gold assets. It is also a live arch-guard demonstration: several violations are deliberate so a pull request produces both deterministic governance findings and advisory LLM review findings.

## Pipeline layout and deliberate violations

`pipelines/ingest.py` defines bronze ingestion plus intentionally misplaced gold and invalid-catalog assets:

- `raw_events` streams `kafka_events` into bronze and has a comment and governance properties. It deliberately has no `@dlt.expect` or `@dlt.expect_or_drop`, allowing the LLM reviewer to demonstrate a data-quality warning.
- `raw_users` streams `s3_landing_zone`. Its decorator name is compliant even though the comment beside it says it is a naming violation; the Python function name `RawUsers` is not used for naming because `name="raw_users"` takes precedence.
- `bad_shortcut` is declared in gold but reads `raw_events` directly. This deliberately triggers `medallion.illegal_read` and omits `comment=`, which the LLM may flag.
- `BadCatalogTable` targets `legacy_warehouse`, uses an uppercase logical name, and lacks the `raw_` bronze prefix. It deliberately triggers `catalog.unsanctioned`, `naming.table`, and `naming.bronze_prefix` and may receive LLM quality/comment findings.
- `__BadCatalogTable` repeats the unsanctioned catalog, starts with underscores, lacks the bronze prefix, and reuses the Python function name `BadCatalogTable`. These are demonstration inputs, including a duplicate-function-name pattern for the LLM reviewer.

`pipelines/transform.py` defines the silver layer:

- `clean_events` reads `raw_events`, parses the payload, and drops records without `event_id` using `@dlt.expect_or_drop`.
- `clean_users` reads `raw_events`, deduplicates it, and filters null IDs. It is structurally compliant, though a reviewer may question whether users should instead derive from `raw_users` or recommend a declarative expectation.

`pipelines/serve.py` defines gold serving assets:

- `events_daily` reads `clean_events` and aggregates daily counts for BI.
- Four repeated definitions of `debug_shortcut` deliberately read bronze `raw_events` directly, triggering `medallion.illegal_read`. Repeated Python definitions and missing comments are also LLM-review material.

`databricks.yml` declares three correctly named DAB pipeline resources—`plp_bronze_events_ingest`, `plp_silver_events_transform`, and `plp_gold_events_serve`—all targeting sanctioned `dev_analytics` and their corresponding schema. `arch-contract.yaml` is the repository's governance source of truth, `.arch-waivers.yaml` is its reviewed exception ledger, and `run_check.sh` reproduces the check locally.

## GitHub Actions integration

The essential reusable-workflow caller is:

```yaml
name: arch-guard
on: {pull_request: {}}
jobs:
  arch-guard:
    uses: gs-dbx/arch-guard/.github/workflows/arch-guard-callable.yml@main
    with:
      contract: arch-contract.yaml
      runner: self-hosted
```

The checked-in `.github/workflows/arch-guard.yml` expands this pattern with path filters, manual dispatch, the `demo` GitHub environment, Databricks inputs, FM review, permissions, and client-credentials authentication.

Configure these under **Settings → Secrets and variables → Actions**:

| Kind | Exact name | Value |
|---|---|---|
| Repository variable | `DATABRICKS_HOST` | Workspace URL such as `https://adb-1234567890123456.7.azuredatabricks.net` |
| Repository variable | `DATABRICKS_CLIENT_ID` | Databricks service-principal application ID |
| Repository variable | `FM_SERVING_ENDPOINT` | Serving endpoint or AI Gateway name, for example `system.ai.claude-opus-5` |
| Repository secret | `DATABRICKS_CLIENT_SECRET` | OAuth secret for that service principal |

Create the `demo` environment under **Settings → Environments** because the workflow assigns the job to it. The reusable workflow also accepts an optional `ARCH_GUARD_TOKEN` secret when the normal `github.token` cannot read the engine repository; this repo does not currently pass it.

The runner must have the `self-hosted` label. Its stable egress IP must be allowed by the Databricks workspace IP ACL, and it must have Git, Bash, curl, and a suitable Python environment. The preferred runner Python is `/opt/databricks/isaac-omni/bin/python3` with `yaml`, `jsonschema`, and `databricks.sdk` already installed.

## Triggering and observing a check

Create a branch, change a path selected by the workflow, push it, and open a PR:

```bash
git switch -c docs/verify-arch-guard
git add pipelines/ingest.py
git commit -m "test: exercise arch-guard review"
git push -u origin docs/verify-arch-guard
```

The workflow triggers for changes under `pipelines/**`, `jobs/**`, `notebooks/**`, any `.sql` file, `databricks.yml`, or `arch-contract.yaml`. In the PR's checks area, open **arch-guard → Details**. In the Actions run, select the `arch-guard` job and its **Summary** page. The summary reports posture, error/warning/waived counts, and a finding table. The SARIF upload may also place annotations on changed lines.

The checked-in workflow is advisory: its expression always resolves `advisory` to true, including manual dispatch. Findings therefore do not fail the job. Enforcement requires changing the caller to pass `advisory: false` and making the check required.

## Reading findings

The **Source** column identifies the tier:

- **Linter** means a deterministic parser/rule produced the result. It is reproducible and can be an error.
- **LLM** means the message starts with `[LLM]` and came from FM review. It is always a warning or note and never blocks.

Severity means:

- `error`: an enforceable contract breach. It blocks only when advisory mode is off and the finding is not waived.
- `warning`: action is recommended but it never controls the checker exit code.
- `note`: lower-priority advisory information.

The rule IDs relevant to this repository are:

| Rule | Meaning |
|---|---|
| `catalog.unsanctioned` | A literal catalog is absent from `sanctioned_catalogs` |
| `naming.table` | A DLT logical name or Spark write target violates `naming.tables.pattern` |
| `naming.bronze_prefix` | An inferred bronze DLT name violates the configured `raw_` prefix |
| `naming.pipelines` | A DAB pipeline name violates the pipeline regex |
| `naming.jobs` | A DAB job name violates the job regex |
| `medallion.illegal_read` | A DLT output reads from a tier not listed in its `may_read_from` policy |
| `parse.syntax_error` | DLT Python could not be parsed |
| `dab.parse_error` | DAB YAML could not be parsed |
| `dlt.quality.*` | LLM data-quality concern, such as a missing expectation |
| `dlt.lineage.*` | LLM lineage/dependency concern, such as `spark.read` inside DLT |
| `dlt.pattern.*` | LLM structural concern, such as missing `comment=` or duplicate functions |
| `dlt.ops.*` | LLM operational concern, such as hard-coded paths or streaming/batch risk |

## Fixing the demonstration findings

### Catalog and bronze naming

In `pipelines/ingest.py`, replace the invalid asset with a sanctioned, lowercase, bronze-prefixed name:

```python
@dlt.table(
    name="raw_catalog_events",
    schema="bronze",
    catalog="dev_analytics",
    comment="Raw catalog event stream from Kafka",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
@dlt.expect_or_drop("valid_event", "value IS NOT NULL")
def raw_catalog_events():
    return dlt.read_stream("kafka_events")
```

This addresses `catalog.unsanctioned`, `naming.table`, `naming.bronze_prefix`, and likely LLM comment/quality findings. Give every decorated asset a unique function name.

### Illegal bronze-to-gold reads

Gold must read silver. Change the shortcut to read `clean_events`, or remove it if it is only for debugging:

```python
@dlt.table(
    name="events_debug",
    schema="gold",
    comment="Debug projection of validated events",
    table_properties={"owner": "analytics", "cost_center": "eng"},
)
def events_debug():
    return dlt.read("clean_events")
```

Use the same correction for `bad_shortcut` and each `debug_shortcut`; retain only one uniquely named definition.

### Missing DLT quality gates

Add a meaningful expectation immediately above the table function. For `raw_events`:

```python
@dlt.table(
    name="raw_events",
    schema="bronze",
    comment="Raw event stream from Kafka",
    table_properties={"owner": "platform", "cost_center": "eng"},
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
def raw_events():
    return (
        dlt.read_stream("kafka_events")
        .selectExpr(
            "CAST(key AS STRING) AS event_id",
            "CAST(value AS STRING) AS payload",
            "timestamp AS ingested_at",
        )
    )
```

Choose `@dlt.expect` when invalid rows should be retained and measured, or `@dlt.expect_or_drop` when the declared policy permits dropping them.

### Missing table comments

Add `comment=` to `bad_shortcut`, `BadCatalogTable`, `__BadCatalogTable`, and `debug_shortcut`. Comments should describe purpose and grain, not repeat the table name:

```python
@dlt.table(
    name="events_daily",
    schema="gold",
    comment="One row per UTC date with the count of validated events for BI",
    table_properties={"owner": "analytics", "cost_center": "eng"},
)
```

### DAB resource naming or catalog findings

Keep resource display names in the configured forms and catalogs sanctioned:

```yaml
resources:
  pipelines:
    plp_gold_events_serve:
      name: plp_gold_events_serve
      catalog: dev_analytics
      target: gold
```

If a job is added, use a display name such as `job_refresh_events_daily`.

## Filing a waiver

Fix code when the contract accurately describes the architecture. File a waiver only for a reviewed false positive, an intentional time-bounded migration, or a risk that cannot be removed in the current change.

1. Copy the exact `rule_id`, repo-relative file, and line from the current job summary.
2. Edit `.arch-waivers.yaml` in the same branch.
3. Add a precise reason, approver, and preferably an ISO expiration date.
4. Commit and push the waiver. arch-guard reruns and moves a matching result into the collapsed waived-findings section.
5. Request review from the named approver. The file is the audit trail; do not merge an unreviewed exception.

A line-specific waiver for the current `bad_shortcut` read is:

```yaml
waivers:
  - rule_id: medallion.illegal_read
    file: pipelines/ingest.py
    line: 41
    reason: >-
      The temporary gold compatibility table must preserve the legacy bronze shape
      during the DATA-4821 consumer migration; it will switch to clean_events after cutover.
    approved_by: data-platform-admins
    expires: 2026-11-01
```

Line numbers move when code is edited. Omitting `line` waives every finding with the same rule and file, which is broader and should be exceptional:

```yaml
waivers:
  - rule_id: dlt.pattern.missing_comment
    file: pipelines/serve.py
    reason: >-
      These compatibility outputs are removed by DATA-4890; catalog descriptions
      remain authoritative until the scheduled 2026-10-15 deletion.
    approved_by: data-platform-admins
    expires: 2026-10-15
```

Matching is exact and case-sensitive. An expired waiver is ignored and logs `arch-guard: waiver for RULE/FILE expired on DATE — treating as active.` Invalid expiration text is currently treated as non-expiring, so reviewers must verify the ISO date.

## Changing the architecture contract

`arch-contract.yaml` is validated on every run. If it changes, arch-guard rescans every tracked supported file rather than only the diff.

- `version` is the integer contract format version.
- `sanctioned_catalogs` lists allowed literal catalog names and their `dev`, `staging`, or `prod` environment. This drives catalog rules.
- `external_sources` documents permitted bronze inputs. The LLM receives the diff but the current deterministic engine does not enforce this list.
- `schemas.required` and `schemas.tier_map` document the expected schema layout. Current tier inference uses literal bronze/silver/gold schema values and name prefixes, not this map.
- `tiering.<tier>.may_read_from` drives `medallion.illegal_read`; `may_write_to` and `serving` document the model but have no current deterministic rule.
- `naming.tables`, `naming.pipelines`, `naming.jobs`, and `naming.bronze_table_prefix` contain regex and severity configuration used by naming rules.
- `required_tags` documents table/pipeline metadata requirements; no registered deterministic rule consumes it yet.
- `overrides` documents the future PR-label policy; current suppression is `.arch-waivers.yaml`, and the checker does not inspect PR labels.

To authorize a newly provisioned catalog:

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

To declare a new external source:

```yaml
external_sources:
  - name: kafka_events
    type: kafka
  - name: s3_landing_zone
    type: cloud_storage
  - name: crm_change_feed
    type: kafka
```

Contract changes should accompany provisioning, ownership, and data-classification review. Adding a name to the YAML does not create the Databricks object or grant access.

## Local checks

Clone `arch-guard` beside this repository and install its dependencies:

```bash
cd /home/greg.skinner/gs-dbx/arch-guard
python3 -m pip install -r requirements.txt
cd /home/greg.skinner/gs-dbx/vanilla-pipeline
./run_check.sh
```

With no arguments, the script diffs the empty Git tree against `HEAD`, checking all tracked files. To reproduce a PR-sized diff:

```bash
./run_check.sh origin/main HEAD
```

It overwrites `findings.sarif` and `summary.md`, prints each finding, and runs advisory. FM review is disabled unless `FM_ENDPOINT` and working Databricks SDK credentials are present. The script uses `python3`, so ensure `yaml`, `jsonschema`, and `databricks.sdk` import in that interpreter.
