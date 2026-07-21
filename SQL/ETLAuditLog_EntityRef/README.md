# SQL — Snowflake ELT Audit & Reference Data Framework

This folder contains two independent Snowflake frameworks:

1. **ELT Audit Framework** (`ELT_Log.sql` + `ETL_LOG_MAST.sql`) — tracks every run of every ELT job/pipeline, regardless of which orchestration tool (Informatica, Matillion, dbt, ...) triggers it.
2. **Reference Table Management** (`Entity_Ref_Table_Mngv2.sql`) — upsert procedures for a company/entity and source-system master data schema, with a full change log.

They don't depend on each other and can be deployed separately.

---

## 1. ELT Audit Framework

Files: `ELT_Log.sql` (core), `ETL_LOG_MAST.sql` (optional pipeline/orchestration add-on).

### Why it exists

ELT tools come and go, but you want one durable audit trail of "what ran, when, and did it succeed" that doesn't change shape when you switch tools. Every tool just calls the same handful of stored procedures; `TOOL_NAME` is a plain column value, not part of the schema.

### Deployment order

```
1. ELT_Log.sql        -- job-level tracking (required)
2. ETL_LOG_MAST.sql    -- pipeline/orchestration tracking (optional, run after #1)
```

Both live in `ANALYTICS_DB.ELT_AUDIT` (replace `ANALYTICS_DB` with your actual database before running).

### Core concepts (`ELT_Log.sql`)

| Table | Purpose |
|---|---|
| `ELT_JOB_CATALOG` | Master list of jobs. One row per pipeline/job definition (name, source system, target table, owning team). Register a job here once, before it ever runs. |
| `ELT_JOB_RUN_LOG` | One row per execution attempt. The core audit trail — timing, row counts, status, who triggered it. `RUN_ID` comes from `SEQ_ELT_JOB_RUN_LOG`. |
| `ELT_ERROR_LOG` | Detailed error records linked to a run (a run can have multiple errors). |

**Stored procedures** — this is the entire surface area an ELT tool needs to know:

- `sp_register_job(p_job_name, p_description, p_source_system, p_target_schema, p_target_table, p_job_category, p_owner_team)` → `VARCHAR`
  Upserts a row into `ELT_JOB_CATALOG`. Idempotent — call it as many times as you like (e.g. on every deploy).

- `sp_start_job_run(p_job_name, p_tool_name, p_tool_job_id, p_environment, p_triggered_by, p_load_window_start, p_load_window_end)` → `NUMBER`
  Call at the start of a job. Looks up the job in the catalog (errors if not registered/inactive), inserts a `RUNNING` row, and returns the new `RUN_ID`. **Store this ID** — you need it for every other call about this run.

- `sp_end_job_run(p_run_id, p_status, p_rows_extracted, p_rows_inserted, p_rows_updated, p_rows_deleted, p_rows_rejected, p_error_message, p_error_code, p_notes)` → `VARCHAR`
  Call at the end of a job, success or failure. `p_status` must be `SUCCESS`, `FAILED`, or `WARNING` — anything else raises an error (see "Gotchas" below for why this validation exists in code, not just as a table constraint).

- `sp_log_error(p_run_id, p_job_name, p_error_message, p_error_detail, p_error_code, p_error_severity, p_source_record)` → `VARCHAR`
  Call any time mid-run to record a specific error (can be called multiple times per run). `p_error_severity` must be `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.

**Reporting views** (query these, never the raw tables):

- `VW_JOB_SUMMARY` — one row per job: total runs, success rate, last run, avg duration. **Scoped to `ENVIRONMENT = 'PROD'`** so a flaky DEV run never skews the production dashboard.
- `VW_FAILED_RUNS` — every failed run with its error detail. First stop when something breaks.
- `VW_RECENT_RUN_HISTORY` — every run (any environment/status) in the last 30 days.
- `VW_ROW_COUNT_ANOMALIES` — flags successful runs whose `ROWS_INSERTED` is more than 2 standard deviations from that job's historical average (z-score). Needs at least 5 prior successful runs before it'll flag anything.

### Typical job lifecycle

```sql
CALL sp_register_job('LOAD_SALES_FACT_DAILY', 'Loads daily sales...', 'Salesforce',
                      'SALES_DW', 'FACT_SALES', 'INCREMENTAL', 'Data Engineering');

-- at the start of each run:
-- v_run_id = CALL sp_start_job_run('LOAD_SALES_FACT_DAILY', 'INFORMATICA', 'WF_...',
--                                   'PROD', 'svc_informatica', window_start, window_end)

-- at the end of each run:
CALL sp_end_job_run(:v_run_id, 'SUCCESS', 50000, 48500, 1200, 0, 300, NULL, NULL, 'ok');
```

### Pipeline / orchestration layer (`ETL_LOG_MAST.sql`)

Adds the concept of a **pipeline**: an orchestrated sequence of jobs run together in one Taskflow/workflow (e.g. "run dims, then facts, then aggregates"). This sits one level above job tracking and answers "did the whole Taskflow succeed, and if not, which step broke it?"

| Table | Purpose |
|---|---|
| `ELT_PIPELINE_CATALOG` | Master list of pipelines/Taskflows. |
| `ELT_PIPELINE_CATALOG_JOBS` | Which jobs belong to which pipeline, in what order, and whether they're optional (`IS_OPTIONAL`). |
| `ELT_PIPELINE_RUN_LOG` | One row per pipeline execution — status, timing, and aggregated row/job counts rolled up from its child job runs. `PIPELINE_RUN_ID` comes from `SEQ_ELT_PIPELINE_RUN_LOG`. |

`ELT_JOB_RUN_LOG` gains two nullable columns: `PIPELINE_RUN_ID` and `STEP_NUMBER`. Standalone jobs (not part of any pipeline) leave both `NULL` and behave exactly as before — fully backwards compatible.

**Additional/replaced procedures:**

- `sp_register_pipeline(...)` → `VARCHAR` — upsert a pipeline definition. Idempotent.
- `sp_add_job_to_pipeline(p_pipeline_name, p_job_name, p_step_order, p_is_optional, p_notes)` → `VARCHAR` — attach a job to a pipeline at a given step. A job can belong to multiple pipelines.
- `sp_start_pipeline_run(p_pipeline_name, p_tool_name, p_tool_taskflow_id, p_environment, p_triggered_by, p_trigger_type)` → `NUMBER` — call once at the start of the Taskflow. Returns `PIPELINE_RUN_ID` — pass it to every `sp_start_job_run` call in this run.
- `sp_start_job_run(...)` — **replaced** version of the core procedure, now takes two extra optional params: `p_pipeline_run_id`, `p_step_number`.
- `sp_skip_job_run(p_job_name, p_tool_name, p_environment, p_triggered_by, p_pipeline_run_id, p_step_number, p_reason)` → `NUMBER` — call **instead of** `sp_start_job_run`/`sp_end_job_run` when an `IS_OPTIONAL` step is deliberately skipped (e.g. a month-end-only job on a non-month-end day). Without this call, a skipped step just looks like it never happened — there's no other way to produce a `SKIPPED` row.
- `sp_end_pipeline_run(p_pipeline_run_id, p_status, p_failed_at_job, p_failed_at_step, p_error_summary, p_notes)` → `VARCHAR` — call once at the end of the Taskflow. `p_status` must be `SUCCESS`, `FAILED`, `PARTIAL`, or `WARNING`. Automatically aggregates row counts and job counts (`JOBS_COMPLETED`/`JOBS_FAILED`/`JOBS_SKIPPED`) from every child job run tagged with this `PIPELINE_RUN_ID`.

**Reporting views:**

- `VW_PIPELINE_RUN_SUMMARY` — top-level health per pipeline (PROD-scoped, same reasoning as `VW_JOB_SUMMARY`).
- `VW_PIPELINE_RUN_HISTORY` — every pipeline run in the last 30 days.
- `VW_PIPELINE_JOB_BREAKDOWN` — for one `PIPELINE_RUN_ID`, every job/step and its individual outcome.
- `VW_PIPELINE_DEFINITION` — a pipeline and all its member jobs in step order; useful for onboarding/documentation.

### Typical pipeline lifecycle

```sql
-- setup, once:
CALL sp_register_pipeline('EOD_FINANCE_PIPELINE', ..., 'Daily 22:00 UTC', 'Finance Data Engineering');
CALL sp_add_job_to_pipeline('EOD_FINANCE_PIPELINE', 'LOAD_ACCOUNT_DIM', 1, FALSE, NULL);
CALL sp_add_job_to_pipeline('EOD_FINANCE_PIPELINE', 'LOAD_TRIAL_BALANCE', 4, TRUE, 'Optional — month-end only');

-- runtime, every execution:
-- v_pipeline_run_id = CALL sp_start_pipeline_run('EOD_FINANCE_PIPELINE', 'INFORMATICA_IICS', taskflow_id, 'PROD', 'svc_informatica', 'SCHEDULED')

-- for each step that actually runs:
-- v_run_id = CALL sp_start_job_run('LOAD_ACCOUNT_DIM', 'INFORMATICA_IICS', session_id, 'PROD', 'svc_informatica', NULL, NULL, :v_pipeline_run_id, 1)
CALL sp_end_job_run(:v_run_id, 'SUCCESS', 0, 5000, 0, 0, 0, NULL, NULL, NULL);

-- for an optional step that's skipped instead:
CALL sp_skip_job_run('LOAD_TRIAL_BALANCE', 'INFORMATICA_IICS', 'PROD', 'svc_informatica',
                      :v_pipeline_run_id, 4, 'Skipped — not month-end');

-- once the whole Taskflow finishes:
CALL sp_end_pipeline_run(:v_pipeline_run_id, 'SUCCESS', NULL, NULL, NULL, 'All steps completed normally');
```

---

## 2. Reference Table Management

Files: `Entity_Ref_Table_Mngv2.sql` (**canonical — use this one**), `Entity_Ref_Table_Mng.sql` (v1, also functional but superseded).

### Why two files

Both `CREATE OR REPLACE` the exact same procedure names in `<DATABASE>.ref`, so whichever you run last "wins." v2 is the actively maintained version; v1 is kept for history. **Only run one of them, and run v2 last if you run both.**

### What it does

Two upsert procedures for master-data tables that are assumed to already exist: `ref.entity` (companies/subsidiaries) and `ref.source_system` (systems that feed your data warehouse). Both procedures:

- Validate every input (required fields, controlled vocabularies, FK existence/active-status) and raise a descriptive, named error on the first thing that's wrong.
- Insert if the natural key doesn't exist yet, update if it does (upsert).
- Log every insert/update to `ref.change_log`, including a `change_id` from `ref.seq_change_log`.
- Return the full affected row plus `action` (`INSERT`/`UPDATE`) and `change_id`.
- Are idempotent — calling twice with the same arguments is safe.

**`sp_upsert_entity`** — params: `p_entity_id, p_entity_name, p_entity_short_code, p_entity_type ('HOLDING'|'SUBSIDIARY'), p_parent_entity_id, p_country_code, p_timezone, p_is_active, p_effective_from, p_effective_to, p_notes, p_updated_by`.
A `HOLDING` entity must have no parent; a `SUBSIDIARY` must have a valid, active parent.

**`sp_upsert_source_system`** — params: `p_source_system_id (lowercase snake_case), p_source_system_name, p_source_system_category ('CRM'|'ERP'|'FILE'|'API'|'DATABASE'|'INTERNAL'), p_description, p_owning_entity_id, p_owner_team, p_owner_contact, p_connection_type ('JDBC'|'REST_API'|'S3'|'SFTP'|'SNOWPIPE'|'INTERNAL'), p_environment ('PROD'|'UAT'|'DEV'), p_is_active, p_onboarded_date, p_decommissioned_date, p_notes, p_updated_by`.

### Typical usage

```sql
-- new subsidiary
CALL <DATABASE>.ref.sp_upsert_entity('ENT-005', 'Acme Southwest LLC', 'ACME_SW', 'SUBSIDIARY',
                                      'ENT-001', 'US', 'America/Denver', TRUE,
                                      '2026-01-01'::DATE, NULL, 'Acquired January 2026', 'data_engineering');

-- decommission an entity: same procedure, just flip is_active and set effective_to

-- new source system
CALL <DATABASE>.ref.sp_upsert_source_system('stripe', 'Stripe Payments', 'API', '...',
                                             'ENT-001', 'Finance Engineering', 'finance-eng@company.com',
                                             'REST_API', 'PROD', TRUE, '2026-03-20'::DATE, NULL, NULL, 'data_engineering');

-- see the audit trail for one record
SELECT * FROM <DATABASE>.ref.change_log WHERE table_name = 'ref.entity' AND record_id = 'ENT-005' ORDER BY change_timestamp DESC;
```

---

## Design decisions worth knowing

- **`RUN_ID` / `PIPELINE_RUN_ID` / `change_id` come from explicit `SEQUENCE.NEXTVAL`**, generated *before* the insert and used directly — not read back afterward with `SELECT MAX(...)`. That pattern would be racy if the same job/entity were touched twice concurrently.
- **`CHECK` constraints on `STATUS`/`ERROR_SEVERITY` are metadata only** — Snowflake never enforces `CHECK`, `UNIQUE`, `PRIMARY KEY`, or `FOREIGN KEY` constraints (only `NOT NULL` is enforced). Real validation happens inside the stored procedures (`sp_end_job_run`, `sp_log_error`, `sp_end_pipeline_run`), which is why those checks exist in code even though the table also declares a `CHECK`.
- **`RAISE` in Snowflake Scripting only takes the name of a previously `DECLARE`d exception** — it does not support inline `RAISE EXCEPTION 'message %', arg` (that's PL/pgSQL syntax, not Snowflake). Every validation failure in these procedures raises its own named, fixed-message exception declared at the top of the procedure body.
- **`VW_JOB_SUMMARY` and `VW_PIPELINE_RUN_SUMMARY` are scoped to `ENVIRONMENT = 'PROD'`** — without that, a noisy DEV/UAT run would drag down the success-rate numbers meant to represent production health. Use `VW_RECENT_RUN_HISTORY` / `VW_PIPELINE_RUN_HISTORY` if you need to see all environments.
