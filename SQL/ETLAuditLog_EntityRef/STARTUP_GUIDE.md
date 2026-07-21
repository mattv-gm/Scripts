# Startup Guide — Deploying This Repo's Snowflake Objects

Step-by-step instructions to get everything in this folder running in a fresh Snowflake account/database. For a full reference of every table, procedure, and view, see [README.md](./README.md).

## 0. Before you start

You need a Snowflake role with, on the target database: `CREATE SCHEMA`, `CREATE TABLE`, `CREATE SEQUENCE`, `CREATE PROCEDURE`, `CREATE VIEW`. `ACCOUNTADMIN` or a standard data-engineering role with `USAGE`+`CREATE *` on the database will work.

Decide up front:
- **What database the ELT audit framework lives in** — every script uses the placeholder `ANALYTICS_DB`.
- **What database your reference/master data lives in** — the ref-table scripts use the placeholder `<DATABASE>`.
- **Whether you need pipeline/orchestration-level tracking** (`ETL_LOG_MAST.sql`) or just job-level tracking (`ELT_Log.sql` alone is enough for that).

These two frameworks are independent — deploy one, both, or neither of the ref-table scripts without affecting the other.

---

## 1. Deploy the ELT audit framework (required)

1. Open `ELT_Log.sql`.
2. Replace `ANALYTICS_DB` (line 37, `USE DATABASE ANALYTICS_DB;`) with your real database name.
3. Run the entire file top to bottom in a Snowflake worksheet.
4. Verify:
   ```sql
   SHOW TABLES IN SCHEMA ANALYTICS_DB.ELT_AUDIT;      -- ELT_JOB_CATALOG, ELT_JOB_RUN_LOG, ELT_ERROR_LOG
   SHOW SEQUENCES IN SCHEMA ANALYTICS_DB.ELT_AUDIT;   -- SEQ_ELT_JOB_RUN_LOG
   SHOW PROCEDURES IN SCHEMA ANALYTICS_DB.ELT_AUDIT;  -- sp_register_job, sp_start_job_run, sp_end_job_run, sp_log_error
   SHOW VIEWS IN SCHEMA ANALYTICS_DB.ELT_AUDIT;       -- VW_JOB_SUMMARY, VW_FAILED_RUNS, VW_RECENT_RUN_HISTORY, VW_ROW_COUNT_ANOMALIES
   ```
5. The file's own Section 5 is a runnable smoke test (register a job, start/end a run, query the views) — see step 4 below for the same walkthrough.

---

## 2. Deploy pipeline/orchestration tracking (optional)

Only needed if you orchestrate multiple jobs together as one Taskflow/workflow and want a top-level "did the whole thing succeed" view.

1. Deploy step 1 first — this file `ALTER`s `ELT_JOB_RUN_LOG` and references `ELT_JOB_CATALOG`.
2. Open `ETL_LOG_MAST.sql`, replace `ANALYTICS_DB` (line 38) the same way.
3. Run the entire file top to bottom.
4. Verify:
   ```sql
   SHOW TABLES IN SCHEMA ANALYTICS_DB.ELT_AUDIT;      -- + ELT_PIPELINE_CATALOG, ELT_PIPELINE_CATALOG_JOBS, ELT_PIPELINE_RUN_LOG
   SHOW PROCEDURES IN SCHEMA ANALYTICS_DB.ELT_AUDIT;  -- + sp_register_pipeline, sp_add_job_to_pipeline, sp_start_pipeline_run, sp_end_pipeline_run, sp_skip_job_run
   DESC TABLE ANALYTICS_DB.ELT_AUDIT.ELT_JOB_RUN_LOG; -- confirm PIPELINE_RUN_ID and STEP_NUMBER columns exist
   ```

This replaces `sp_start_job_run` with a version that takes two extra optional parameters (`p_pipeline_run_id`, `p_step_number`) — standalone jobs that don't pass them behave exactly as before.

---

## 3. Deploy reference table management (optional, independent)

Only run **`Entity_Ref_Table_Mngv2.sql`** — it's the canonical, current version. Do not run `Entity_Ref_Table_Mng.sql` (v1) afterward, or it will silently overwrite v2's procedures with the older, less complete ones. (See [README.md](./README.md#2-reference-table-management) for why both files exist.)

**Prerequisite:** this script only creates `ref.change_log` — it assumes `ref.entity` and `ref.source_system` already exist. If they don't yet, create something like this first (adjust types/constraints to your needs):

```sql
CREATE SCHEMA IF NOT EXISTS <DATABASE>.ref;

CREATE TABLE IF NOT EXISTS <DATABASE>.ref.entity (
    entity_id           VARCHAR(100)  PRIMARY KEY,
    entity_name         VARCHAR(500)  NOT NULL,
    entity_short_code   VARCHAR(50)   NOT NULL,
    entity_type         VARCHAR(20)   NOT NULL,   -- HOLDING | SUBSIDIARY
    parent_entity_id    VARCHAR(100),
    country_code        VARCHAR(2),
    timezone            VARCHAR(100),
    is_active           BOOLEAN       NOT NULL DEFAULT TRUE,
    effective_from      DATE          NOT NULL,
    effective_to        DATE,
    notes               VARCHAR(2000),
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    updated_by          VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS <DATABASE>.ref.source_system (
    source_system_id        VARCHAR(100)  PRIMARY KEY,
    source_system_name      VARCHAR(500)  NOT NULL,
    source_system_category  VARCHAR(20)   NOT NULL,  -- CRM | ERP | FILE | API | DATABASE | INTERNAL
    description              VARCHAR(2000),
    owning_entity_id        VARCHAR(100),
    owner_team               VARCHAR(200),
    owner_contact            VARCHAR(200),
    connection_type          VARCHAR(20),             -- JDBC | REST_API | S3 | SFTP | SNOWPIPE | INTERNAL
    environment              VARCHAR(10),              -- PROD | UAT | DEV
    is_active                BOOLEAN       NOT NULL DEFAULT TRUE,
    onboarded_date           DATE          NOT NULL,
    decommissioned_date      DATE,
    notes                    VARCHAR(2000),
    created_at               TIMESTAMP_NTZ,
    updated_at               TIMESTAMP_NTZ,
    updated_by               VARCHAR(100)
);
```

Then:

1. Open `Entity_Ref_Table_Mngv2.sql`, find/replace every `<DATABASE>` with your real database name.
2. Run the entire file top to bottom.
3. Verify:
   ```sql
   SHOW TABLES IN SCHEMA <DATABASE>.ref;      -- change_log
   SHOW SEQUENCES IN SCHEMA <DATABASE>.ref;   -- seq_change_log
   SHOW PROCEDURES IN SCHEMA <DATABASE>.ref;  -- sp_upsert_entity, sp_upsert_source_system
   ```

---

## 4. Smoke test the ELT audit framework

Copy-paste this after step 1 (and step 2, if deployed) to confirm everything works end to end:

```sql
USE DATABASE ANALYTICS_DB;
USE SCHEMA ELT_AUDIT;

-- 1. Register a test job
CALL sp_register_job(
    'TEST_JOB', 'Startup guide smoke test', 'Manual', 'TEST_SCHEMA', 'TEST_TABLE',
    'FULL_LOAD', 'Data Engineering'
);

-- 2. Start a run — note the returned RUN_ID
CALL sp_start_job_run('TEST_JOB', 'MANUAL', 'smoke-test-1', 'DEV', CURRENT_USER());

-- 3. End the run (replace 1 with the RUN_ID returned above)
CALL sp_end_job_run(1, 'SUCCESS', 100, 100, 0, 0, 0, NULL, NULL, 'Smoke test passed');

-- 4. Confirm it shows up
SELECT * FROM VW_RECENT_RUN_HISTORY WHERE JOB_NAME = 'TEST_JOB';
```

If that last query returns one row with `STATUS = 'SUCCESS'`, the framework is working. Clean up the test job afterward if you like:

```sql
UPDATE ELT_JOB_CATALOG SET IS_ACTIVE = FALSE WHERE JOB_NAME = 'TEST_JOB';
```

---

## 5. Wire it into your ELT tool

The stored procedure names never change when you switch tools — only the `p_tool_name` value and where you place the calls. See the "HOW TO INTEGRATE WITH YOUR ELT TOOL" block at the bottom of `ELT_Log.sql` for Informatica/Matillion/dbt hook points, and the [README.md](./README.md) lifecycle examples for the full call sequence (including the pipeline layer, if deployed).

---

## Troubleshooting

- **`sp_start_job_run: Job not found in catalog or is inactive.`** — you forgot to call `sp_register_job` first, or the job's `IS_ACTIVE` flag is `FALSE`.
- **`sp_end_job_run: p_status must be one of SUCCESS, FAILED, WARNING.`** (or the pipeline/severity equivalents) — check spelling/case; these are validated in the procedure body, not just documented.
- **Re-running a script** — every `CREATE TABLE`/`CREATE SEQUENCE` uses `IF NOT EXISTS` and every `CREATE PROCEDURE`/`CREATE VIEW` uses `OR REPLACE`, so re-running any of these files is safe and won't duplicate objects or lose data.
- **Ran `Entity_Ref_Table_Mng.sql` (v1) by accident after v2?** Just re-run `Entity_Ref_Table_Mngv2.sql` — `CREATE OR REPLACE PROCEDURE` will restore the canonical version.
