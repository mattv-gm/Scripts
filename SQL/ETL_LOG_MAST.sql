/*
================================================================================
  ELT AUDIT FRAMEWORK — PIPELINE / ORCHESTRATION CATALOG EXTENSION
  Add-on to snowflake_elt_audit_framework.sql

  PURPOSE
  ───────
  Adds the concept of a "pipeline" — an orchestrated sequence of jobs that run
  together inside a single Taskflow or workflow. This sits one level above the
  existing job-level tracking and answers questions like:
    - Did the entire end-of-day finance pipeline succeed?
    - Which step did it fail on?
    - How long did the whole thing take end to end?
    - How many rows were loaded across all jobs in the pipeline?

  DESIGN DECISIONS
  ────────────────
  1. Backwards compatible — ELT_JOB_RUN_LOG gets a nullable PIPELINE_RUN_ID.
     Standalone jobs that don't belong to a pipeline leave it NULL and behave
     exactly as before.

  2. A job can belong to multiple pipelines — the membership table
     ELT_PIPELINE_CATALOG_JOBS is the join table. One row per job-pipeline pair.

  3. IS_OPTIONAL on job membership — optional jobs that are skipped do not
     fail the overall pipeline. Required jobs (IS_OPTIONAL = FALSE) that fail
     roll the whole pipeline to FAILED.

  4. STEP_ORDER on membership — defines the expected sequence for display
     purposes. Does not enforce execution order (that is the tool's job).

  SETUP ORDER
  ───────────
  Run AFTER snowflake_elt_audit_framework.sql. Sections run in order: 1 → 2 → 3 → 4
================================================================================
*/

USE DATABASE ANALYTICS_DB;
USE SCHEMA ELT_AUDIT;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 1 — TABLES
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 1a. PIPELINE CATALOG ───────────────────────────────────────────────────
-- One row per orchestration pipeline / Taskflow definition.

CREATE TABLE IF NOT EXISTS ELT_PIPELINE_CATALOG (
    PIPELINE_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME       VARCHAR(200)  NOT NULL,  -- e.g. 'EOD_FINANCE_PIPELINE'
    PIPELINE_DESCRIPTION VARCHAR(1000),
    TOOL_NAME           VARCHAR(100),            -- 'INFORMATICA_IICS', 'MATILLION', etc.
    TOOL_TASKFLOW_NAME  VARCHAR(500),            -- the exact name of the Taskflow/workflow in the tool
    SCHEDULE            VARCHAR(200),            -- e.g. 'Daily 22:00 UTC', 'Hourly', 'On demand'
    OWNER_TEAM          VARCHAR(200),
    IS_ACTIVE           BOOLEAN DEFAULT TRUE,
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT uq_pipeline_name UNIQUE (PIPELINE_NAME)
)
COMMENT = 'Master catalog of all registered orchestration pipelines / Taskflows.';


-- ── 1b. PIPELINE JOB MEMBERSHIP ────────────────────────────────────────────
-- Maps jobs to pipelines. One row per job-pipeline combination.
-- A job can appear in multiple pipelines. A pipeline can contain many jobs.

CREATE TABLE IF NOT EXISTS ELT_PIPELINE_CATALOG_JOBS (
    ID                  NUMBER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_ID         NUMBER        NOT NULL REFERENCES ELT_PIPELINE_CATALOG(PIPELINE_ID),
    JOB_ID              NUMBER        NOT NULL REFERENCES ELT_JOB_CATALOG(JOB_ID),
    STEP_ORDER          NUMBER        NOT NULL,   -- expected sequence: 1, 2, 3...
    IS_OPTIONAL         BOOLEAN DEFAULT FALSE,    -- FALSE = failure here fails the pipeline
    NOTES               VARCHAR(500),             -- e.g. 'Runs only on month-end'

    CONSTRAINT uq_pipeline_job UNIQUE (PIPELINE_ID, JOB_ID)
)
COMMENT = 'Membership table: which jobs belong to which pipelines and in what order.';


-- ── 1c. PIPELINE RUN LOG ───────────────────────────────────────────────────
-- One row per pipeline execution. Provides the top-level view of whether
-- the whole orchestration succeeded or failed.

CREATE TABLE IF NOT EXISTS ELT_PIPELINE_RUN_LOG (
    PIPELINE_RUN_ID     NUMBER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_ID         NUMBER        NOT NULL REFERENCES ELT_PIPELINE_CATALOG(PIPELINE_ID),
    PIPELINE_NAME       VARCHAR(200)  NOT NULL,   -- denormalised for easy querying
    TOOL_NAME           VARCHAR(100),
    TOOL_TASKFLOW_ID    VARCHAR(500),             -- the run/instance ID from the tool
    ENVIRONMENT         VARCHAR(50)   DEFAULT 'PROD',
    TRIGGERED_BY        VARCHAR(200),
    TRIGGER_TYPE        VARCHAR(50),              -- 'SCHEDULED', 'MANUAL', 'API', 'EVENT'

    -- Timing
    START_TIME          TIMESTAMP_NTZ NOT NULL,
    END_TIME            TIMESTAMP_NTZ,
    DURATION_SECONDS    NUMBER AS (
                            DATEDIFF('second', START_TIME, COALESCE(END_TIME, CURRENT_TIMESTAMP()))
                        ),

    -- Outcome
    -- Valid values: RUNNING | SUCCESS | FAILED | PARTIAL | WARNING
    -- Enforced in sp_end_pipeline_run() — Snowflake does not support CHECK constraints
    STATUS              VARCHAR(20)   NOT NULL,

    -- Failure detail
    FAILED_AT_JOB_NAME  VARCHAR(200),            -- which job caused the failure
    FAILED_AT_STEP      NUMBER,                  -- which step number failed
    ERROR_SUMMARY       VARCHAR(2000),

    -- Aggregate row counts across all child jobs
    TOTAL_ROWS_INSERTED NUMBER DEFAULT 0,
    TOTAL_ROWS_UPDATED  NUMBER DEFAULT 0,
    TOTAL_ROWS_REJECTED NUMBER DEFAULT 0,

    JOBS_TOTAL          NUMBER DEFAULT 0,        -- how many jobs were expected
    JOBS_COMPLETED      NUMBER DEFAULT 0,        -- how many completed successfully
    JOBS_FAILED         NUMBER DEFAULT 0,
    JOBS_SKIPPED        NUMBER DEFAULT 0,

    NOTES               VARCHAR(2000),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'One row per pipeline execution. Top-level view of Taskflow/orchestration outcomes.'
CLUSTER BY (PIPELINE_ID, START_TIME);


-- ── 1d. ADD PIPELINE_RUN_ID TO JOB RUN LOG ────────────────────────────────
-- Link each job run back to the pipeline run it belongs to.
-- NULL for standalone jobs — fully backwards compatible.

ALTER TABLE ELT_JOB_RUN_LOG
    ADD COLUMN IF NOT EXISTS PIPELINE_RUN_ID NUMBER    -- FK to ELT_PIPELINE_RUN_LOG
    REFERENCES ELT_PIPELINE_RUN_LOG(PIPELINE_RUN_ID);

ALTER TABLE ELT_JOB_RUN_LOG
    ADD COLUMN IF NOT EXISTS STEP_NUMBER NUMBER;       -- which step this job was in the pipeline


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 2 — STORED PROCEDURES
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 2a. REGISTER A PIPELINE ────────────────────────────────────────────────
-- Call once when setting up a new Taskflow. Idempotent — safe to call again.

CREATE OR REPLACE PROCEDURE sp_register_pipeline(
    p_pipeline_name         VARCHAR,
    p_description           VARCHAR,
    p_tool_name             VARCHAR,
    p_tool_taskflow_name    VARCHAR,
    p_schedule              VARCHAR,
    p_owner_team            VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO ELT_PIPELINE_CATALOG AS target
    USING (
        SELECT
            :p_pipeline_name        AS PIPELINE_NAME,
            :p_description          AS PIPELINE_DESCRIPTION,
            :p_tool_name            AS TOOL_NAME,
            :p_tool_taskflow_name   AS TOOL_TASKFLOW_NAME,
            :p_schedule             AS SCHEDULE,
            :p_owner_team           AS OWNER_TEAM
    ) AS source ON target.PIPELINE_NAME = source.PIPELINE_NAME
    WHEN MATCHED THEN UPDATE SET
        PIPELINE_DESCRIPTION  = source.PIPELINE_DESCRIPTION,
        TOOL_NAME             = source.TOOL_NAME,
        TOOL_TASKFLOW_NAME    = source.TOOL_TASKFLOW_NAME,
        SCHEDULE              = source.SCHEDULE,
        OWNER_TEAM            = source.OWNER_TEAM,
        UPDATED_AT            = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        PIPELINE_NAME, PIPELINE_DESCRIPTION, TOOL_NAME,
        TOOL_TASKFLOW_NAME, SCHEDULE, OWNER_TEAM
    ) VALUES (
        source.PIPELINE_NAME, source.PIPELINE_DESCRIPTION, source.TOOL_NAME,
        source.TOOL_TASKFLOW_NAME, source.SCHEDULE, source.OWNER_TEAM
    );

    RETURN 'Pipeline registered: ' || :p_pipeline_name;
END;
$$
COMMENT = 'Register or update a pipeline in the catalog. Safe to call multiple times.';


-- ── 2b. ADD JOB TO PIPELINE ────────────────────────────────────────────────
-- Call once per job-pipeline pair during setup.
-- Safe to call again — updates step_order and is_optional if already exists.

CREATE OR REPLACE PROCEDURE sp_add_job_to_pipeline(
    p_pipeline_name VARCHAR,
    p_job_name      VARCHAR,
    p_step_order    NUMBER,
    p_is_optional   BOOLEAN DEFAULT FALSE,
    p_notes         VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_pipeline_id NUMBER;
    v_job_id      NUMBER;
BEGIN
    SELECT PIPELINE_ID INTO v_pipeline_id
    FROM ELT_PIPELINE_CATALOG
    WHERE PIPELINE_NAME = :p_pipeline_name AND IS_ACTIVE = TRUE;

    IF (v_pipeline_id IS NULL) THEN
        RAISE EXCEPTION 'Pipeline not found: %. Run sp_register_pipeline first.', :p_pipeline_name;
    END IF;

    SELECT JOB_ID INTO v_job_id
    FROM ELT_JOB_CATALOG
    WHERE JOB_NAME = :p_job_name AND IS_ACTIVE = TRUE;

    IF (v_job_id IS NULL) THEN
        RAISE EXCEPTION 'Job not found: %. Run sp_register_job first.', :p_job_name;
    END IF;

    MERGE INTO ELT_PIPELINE_CATALOG_JOBS AS target
    USING (
        SELECT :v_pipeline_id AS PIPELINE_ID, :v_job_id AS JOB_ID,
               :p_step_order AS STEP_ORDER, :p_is_optional AS IS_OPTIONAL,
               :p_notes AS NOTES
    ) AS source ON target.PIPELINE_ID = source.PIPELINE_ID
               AND target.JOB_ID      = source.JOB_ID
    WHEN MATCHED THEN UPDATE SET
        STEP_ORDER  = source.STEP_ORDER,
        IS_OPTIONAL = source.IS_OPTIONAL,
        NOTES       = source.NOTES
    WHEN NOT MATCHED THEN INSERT (
        PIPELINE_ID, JOB_ID, STEP_ORDER, IS_OPTIONAL, NOTES
    ) VALUES (
        source.PIPELINE_ID, source.JOB_ID, source.STEP_ORDER, source.IS_OPTIONAL, source.NOTES
    );

    RETURN :p_job_name || ' added to ' || :p_pipeline_name || ' at step ' || :p_step_order;
END;
$$
COMMENT = 'Add or update a job membership in a pipeline. Safe to call multiple times.';


-- ── 2c. START A PIPELINE RUN ───────────────────────────────────────────────
-- Call at the very beginning of the Taskflow, before any job runs.
-- Returns PIPELINE_RUN_ID — store in a Taskflow variable and pass to every
-- sp_start_job_run() call and to sp_end_pipeline_run() at the end.

CREATE OR REPLACE PROCEDURE sp_start_pipeline_run(
    p_pipeline_name     VARCHAR,
    p_tool_name         VARCHAR,
    p_tool_taskflow_id  VARCHAR,
    p_environment       VARCHAR,
    p_triggered_by      VARCHAR,
    p_trigger_type      VARCHAR DEFAULT 'SCHEDULED'
)
RETURNS NUMBER  -- returns PIPELINE_RUN_ID
LANGUAGE SQL
AS
$$
DECLARE
    v_pipeline_id     NUMBER;
    v_jobs_total      NUMBER;
    v_pipeline_run_id NUMBER;
BEGIN
    SELECT PIPELINE_ID INTO v_pipeline_id
    FROM ELT_PIPELINE_CATALOG
    WHERE PIPELINE_NAME = :p_pipeline_name AND IS_ACTIVE = TRUE;

    IF (v_pipeline_id IS NULL) THEN
        RAISE EXCEPTION 'Pipeline not found: %. Run sp_register_pipeline first.', :p_pipeline_name;
    END IF;

    -- Count how many jobs are expected in this pipeline
    SELECT COUNT(*) INTO v_jobs_total
    FROM ELT_PIPELINE_CATALOG_JOBS
    WHERE PIPELINE_ID = v_pipeline_id;

    INSERT INTO ELT_PIPELINE_RUN_LOG (
        PIPELINE_ID, PIPELINE_NAME, TOOL_NAME, TOOL_TASKFLOW_ID,
        ENVIRONMENT, TRIGGERED_BY, TRIGGER_TYPE,
        START_TIME, STATUS, JOBS_TOTAL
    )
    VALUES (
        v_pipeline_id, :p_pipeline_name, UPPER(:p_tool_name), :p_tool_taskflow_id,
        UPPER(:p_environment), :p_triggered_by, UPPER(:p_trigger_type),
        CURRENT_TIMESTAMP(), 'RUNNING', v_jobs_total
    );

    SELECT MAX(PIPELINE_RUN_ID) INTO v_pipeline_run_id
    FROM ELT_PIPELINE_RUN_LOG
    WHERE PIPELINE_NAME = :p_pipeline_name
      AND STATUS = 'RUNNING'
      AND TRIGGERED_BY = :p_triggered_by;

    RETURN v_pipeline_run_id;
END;
$$
COMMENT = 'Opens a pipeline run record. Returns PIPELINE_RUN_ID — pass to every sp_start_job_run call and to sp_end_pipeline_run.';


-- ── 2d. END A PIPELINE RUN ─────────────────────────────────────────────────
-- Call at the very end of the Taskflow, after all jobs have completed.
-- Automatically aggregates row counts from all child job runs.
-- Valid status values: SUCCESS | FAILED | PARTIAL | WARNING

CREATE OR REPLACE PROCEDURE sp_end_pipeline_run(
    p_pipeline_run_id   NUMBER,
    p_status            VARCHAR,
    p_failed_at_job     VARCHAR DEFAULT NULL,
    p_failed_at_step    NUMBER  DEFAULT NULL,
    p_error_summary     VARCHAR DEFAULT NULL,
    p_notes             VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted NUMBER DEFAULT 0;
    v_rows_updated  NUMBER DEFAULT 0;
    v_rows_rejected NUMBER DEFAULT 0;
    v_jobs_completed NUMBER DEFAULT 0;
    v_jobs_failed   NUMBER DEFAULT 0;
    v_jobs_skipped  NUMBER DEFAULT 0;
BEGIN
    -- Validate status
    IF (UPPER(:p_status) NOT IN ('SUCCESS', 'FAILED', 'PARTIAL', 'WARNING')) THEN
        RAISE EXCEPTION 'Invalid status "%". Must be one of: SUCCESS, FAILED, PARTIAL, WARNING.', :p_status;
    END IF;

    -- Aggregate stats from all child job runs in this pipeline run
    SELECT
        COALESCE(SUM(ROWS_INSERTED), 0),
        COALESCE(SUM(ROWS_UPDATED), 0),
        COALESCE(SUM(ROWS_REJECTED), 0),
        COALESCE(SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN STATUS = 'FAILED'  THEN 1 ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN STATUS = 'SKIPPED' THEN 1 ELSE 0 END), 0)
    INTO
        v_rows_inserted, v_rows_updated, v_rows_rejected,
        v_jobs_completed, v_jobs_failed, v_jobs_skipped
    FROM ELT_JOB_RUN_LOG
    WHERE PIPELINE_RUN_ID = :p_pipeline_run_id;

    UPDATE ELT_PIPELINE_RUN_LOG
    SET
        END_TIME            = CURRENT_TIMESTAMP(),
        STATUS              = UPPER(:p_status),
        FAILED_AT_JOB_NAME  = :p_failed_at_job,
        FAILED_AT_STEP      = :p_failed_at_step,
        ERROR_SUMMARY       = :p_error_summary,
        TOTAL_ROWS_INSERTED = v_rows_inserted,
        TOTAL_ROWS_UPDATED  = v_rows_updated,
        TOTAL_ROWS_REJECTED = v_rows_rejected,
        JOBS_COMPLETED      = v_jobs_completed,
        JOBS_FAILED         = v_jobs_failed,
        JOBS_SKIPPED        = v_jobs_skipped,
        NOTES               = :p_notes
    WHERE PIPELINE_RUN_ID = :p_pipeline_run_id;

    RETURN 'Pipeline run ' || :p_pipeline_run_id || ' closed with status: ' || UPPER(:p_status);
END;
$$
COMMENT = 'Closes a pipeline run. Auto-aggregates row counts from all child job runs.';


-- ── 2e. UPDATE sp_start_job_run TO ACCEPT PIPELINE_RUN_ID ─────────────────
-- Replaces the existing sp_start_job_run to add the optional pipeline_run_id
-- and step_number parameters. Backwards compatible — both default to NULL.

CREATE OR REPLACE PROCEDURE sp_start_job_run(
    p_job_name              VARCHAR,
    p_tool_name             VARCHAR,
    p_tool_job_id           VARCHAR,
    p_environment           VARCHAR,
    p_triggered_by          VARCHAR,
    p_load_window_start     TIMESTAMP_NTZ DEFAULT NULL,
    p_load_window_end       TIMESTAMP_NTZ DEFAULT NULL,
    p_pipeline_run_id       NUMBER        DEFAULT NULL,  -- NEW: link to pipeline run
    p_step_number           NUMBER        DEFAULT NULL   -- NEW: step within the pipeline
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
DECLARE
    v_job_id NUMBER;
    v_run_id NUMBER;
BEGIN
    SELECT JOB_ID INTO v_job_id
    FROM ELT_JOB_CATALOG
    WHERE JOB_NAME = :p_job_name AND IS_ACTIVE = TRUE;

    IF (v_job_id IS NULL) THEN
        RAISE EXCEPTION 'Job not found in catalog: %. Run sp_register_job first.', :p_job_name;
    END IF;

    INSERT INTO ELT_JOB_RUN_LOG (
        JOB_ID, JOB_NAME, TOOL_NAME, TOOL_JOB_ID,
        ENVIRONMENT, START_TIME, STATUS,
        TRIGGERED_BY, LOAD_WINDOW_START, LOAD_WINDOW_END,
        PIPELINE_RUN_ID, STEP_NUMBER
    )
    VALUES (
        v_job_id, :p_job_name, UPPER(:p_tool_name), :p_tool_job_id,
        UPPER(:p_environment), CURRENT_TIMESTAMP(), 'RUNNING',
        :p_triggered_by, :p_load_window_start, :p_load_window_end,
        :p_pipeline_run_id, :p_step_number
    );

    SELECT MAX(RUN_ID) INTO v_run_id
    FROM ELT_JOB_RUN_LOG
    WHERE JOB_NAME    = :p_job_name
      AND STATUS      = 'RUNNING'
      AND TRIGGERED_BY = :p_triggered_by;

    RETURN v_run_id;
END;
$$
COMMENT = 'Opens a job run record. Pass p_pipeline_run_id to link to a parent pipeline run.';


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 3 — REPORTING VIEWS
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 3a. PIPELINE RUN SUMMARY ───────────────────────────────────────────────
-- Top-level health check across all pipelines.

CREATE OR REPLACE VIEW VW_PIPELINE_RUN_SUMMARY AS
SELECT
    c.PIPELINE_NAME,
    c.TOOL_NAME,
    c.SCHEDULE,
    c.OWNER_TEAM,
    COUNT(r.PIPELINE_RUN_ID)                                          AS TOTAL_RUNS,
    SUM(CASE WHEN r.STATUS = 'SUCCESS' THEN 1 ELSE 0 END)            AS SUCCESSFUL_RUNS,
    SUM(CASE WHEN r.STATUS = 'FAILED'  THEN 1 ELSE 0 END)            AS FAILED_RUNS,
    ROUND(
        SUM(CASE WHEN r.STATUS = 'SUCCESS' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(r.PIPELINE_RUN_ID), 0) * 100, 1
    )                                                                  AS SUCCESS_RATE_PCT,
    MAX(r.START_TIME)                                                  AS LAST_RUN_START,
    MAX(CASE WHEN r.STATUS = 'SUCCESS' THEN r.END_TIME END)           AS LAST_SUCCESSFUL_RUN,
    ROUND(AVG(r.DURATION_SECONDS), 0)                                  AS AVG_DURATION_SECONDS,
    SUM(r.TOTAL_ROWS_INSERTED)                                         AS ALL_TIME_ROWS_INSERTED
FROM ELT_PIPELINE_CATALOG c
LEFT JOIN ELT_PIPELINE_RUN_LOG r ON c.PIPELINE_ID = r.PIPELINE_ID
WHERE c.IS_ACTIVE = TRUE
GROUP BY 1,2,3,4
ORDER BY c.PIPELINE_NAME;


-- ── 3b. PIPELINE RUN HISTORY ───────────────────────────────────────────────
-- Every pipeline run in the last 30 days with job-level breakdown.

CREATE OR REPLACE VIEW VW_PIPELINE_RUN_HISTORY AS
SELECT
    r.PIPELINE_RUN_ID,
    r.PIPELINE_NAME,
    r.TOOL_NAME,
    r.ENVIRONMENT,
    r.TRIGGER_TYPE,
    r.STATUS,
    r.START_TIME,
    r.END_TIME,
    r.DURATION_SECONDS,
    r.JOBS_TOTAL,
    r.JOBS_COMPLETED,
    r.JOBS_FAILED,
    r.JOBS_SKIPPED,
    r.TOTAL_ROWS_INSERTED,
    r.TOTAL_ROWS_UPDATED,
    r.TOTAL_ROWS_REJECTED,
    r.FAILED_AT_JOB_NAME,
    r.FAILED_AT_STEP,
    r.ERROR_SUMMARY,
    r.TRIGGERED_BY
FROM ELT_PIPELINE_RUN_LOG r
WHERE r.START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY r.START_TIME DESC;


-- ── 3c. PIPELINE JOB BREAKDOWN ─────────────────────────────────────────────
-- For a given pipeline run, shows every job that ran with its individual result.
-- Usage: SELECT * FROM VW_PIPELINE_JOB_BREAKDOWN WHERE PIPELINE_RUN_ID = 5;

CREATE OR REPLACE VIEW VW_PIPELINE_JOB_BREAKDOWN AS
SELECT
    j.PIPELINE_RUN_ID,
    p.PIPELINE_NAME,
    j.STEP_NUMBER,
    pcj.IS_OPTIONAL,
    j.JOB_NAME,
    j.STATUS                                    AS JOB_STATUS,
    j.START_TIME                                AS JOB_START,
    j.END_TIME                                  AS JOB_END,
    j.DURATION_SECONDS                          AS JOB_DURATION_SECONDS,
    j.ROWS_INSERTED,
    j.ROWS_UPDATED,
    j.ROWS_REJECTED,
    j.ERROR_MESSAGE
FROM ELT_JOB_RUN_LOG j
JOIN ELT_PIPELINE_RUN_LOG p  ON j.PIPELINE_RUN_ID = p.PIPELINE_RUN_ID
LEFT JOIN ELT_PIPELINE_CATALOG_JOBS pcj
    ON p.PIPELINE_ID   = pcj.PIPELINE_ID
    AND j.JOB_ID        = pcj.JOB_ID
WHERE j.PIPELINE_RUN_ID IS NOT NULL
ORDER BY j.PIPELINE_RUN_ID, j.STEP_NUMBER NULLS LAST, j.START_TIME;


-- ── 3d. PIPELINE CATALOG WITH JOB LIST ─────────────────────────────────────
-- Shows each pipeline and all its member jobs in step order.
-- Useful for onboarding and documentation.

CREATE OR REPLACE VIEW VW_PIPELINE_DEFINITION AS
SELECT
    c.PIPELINE_NAME,
    c.PIPELINE_DESCRIPTION,
    c.TOOL_NAME,
    c.TOOL_TASKFLOW_NAME,
    c.SCHEDULE,
    c.OWNER_TEAM,
    pcj.STEP_ORDER,
    jc.JOB_NAME,
    jc.SOURCE_SYSTEM,
    jc.TARGET_SCHEMA || '.' || jc.TARGET_TABLE AS TARGET_TABLE,
    jc.JOB_CATEGORY,
    pcj.IS_OPTIONAL,
    pcj.NOTES                                   AS STEP_NOTES
FROM ELT_PIPELINE_CATALOG c
JOIN ELT_PIPELINE_CATALOG_JOBS pcj ON c.PIPELINE_ID = pcj.PIPELINE_ID
JOIN ELT_JOB_CATALOG jc            ON pcj.JOB_ID    = jc.JOB_ID
WHERE c.IS_ACTIVE = TRUE
  AND jc.IS_ACTIVE = TRUE
ORDER BY c.PIPELINE_NAME, pcj.STEP_ORDER;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 4 — EXAMPLE USAGE
   ───────────────────────────────────────────────────────────────────────────*/

-- ── SETUP (run once per pipeline) ──────────────────────────────────────────

-- 1. Register the pipeline
CALL sp_register_pipeline(
    'EOD_FINANCE_PIPELINE',
    'End of day finance load: dims then facts then aggregates',
    'INFORMATICA_IICS',
    'TF_EOD_FINANCE',           -- the Taskflow name inside IICS
    'Daily 22:00 UTC',
    'Finance Data Engineering'
);

-- 2. Register the individual jobs (using existing sp_register_job)
CALL sp_register_job('LOAD_ACCOUNT_DIM',   'Accounts dimension', 'Oracle ERP', 'FINANCE_DW', 'DIM_ACCOUNT',   'FULL_LOAD',    'Finance Data Engineering');
CALL sp_register_job('LOAD_COST_CENTRE_DIM','Cost centres',      'Oracle ERP', 'FINANCE_DW', 'DIM_COST_CENTRE','FULL_LOAD',   'Finance Data Engineering');
CALL sp_register_job('LOAD_GL_FACT',        'General ledger',    'Oracle ERP', 'FINANCE_DW', 'FACT_GL',        'INCREMENTAL', 'Finance Data Engineering');
CALL sp_register_job('LOAD_TRIAL_BALANCE',  'Trial balance agg', 'Snowflake',  'FINANCE_DW', 'AGG_TRIAL_BALANCE','INCREMENTAL','Finance Data Engineering');

-- 3. Define the pipeline membership and step order
CALL sp_add_job_to_pipeline('EOD_FINANCE_PIPELINE', 'LOAD_ACCOUNT_DIM',    1, FALSE, NULL);
CALL sp_add_job_to_pipeline('EOD_FINANCE_PIPELINE', 'LOAD_COST_CENTRE_DIM',2, FALSE, NULL);
CALL sp_add_job_to_pipeline('EOD_FINANCE_PIPELINE', 'LOAD_GL_FACT',        3, FALSE, NULL);
CALL sp_add_job_to_pipeline('EOD_FINANCE_PIPELINE', 'LOAD_TRIAL_BALANCE',  4, TRUE,  'Optional — skipped on non-month-end days');

-- Confirm the pipeline definition looks correct
SELECT * FROM VW_PIPELINE_DEFINITION WHERE PIPELINE_NAME = 'EOD_FINANCE_PIPELINE';


-- ── RUNTIME (what your Taskflow calls on every execution) ──────────────────

-- At the START of the Taskflow — store the returned PIPELINE_RUN_ID in a variable
CALL sp_start_pipeline_run(
    'EOD_FINANCE_PIPELINE',
    'INFORMATICA_IICS',
    'TF_EOD_FINANCE_20240115_220001',   -- unique Taskflow instance ID from IICS
    'PROD',
    'svc_informatica',
    'SCHEDULED'
);
-- Returns e.g. 7 — store as v_pipeline_run_id in the Taskflow

-- For EACH job within the Taskflow, pass the pipeline_run_id and step number:
CALL sp_start_job_run(
    'LOAD_ACCOUNT_DIM',
    'INFORMATICA_IICS',
    'M_LOAD_ACCOUNT_DIM_RUN_001',
    'PROD',
    'svc_informatica',
    NULL, NULL,
    7,   -- p_pipeline_run_id
    1    -- p_step_number
);
-- Returns the JOB RUN_ID — store as v_run_id_step1

-- ... run the mapping ...

CALL sp_end_job_run(
    101,        -- v_run_id_step1
    'SUCCESS',
    0, 5000, 0, 0, 0,
    NULL, NULL, NULL
);

-- Repeat sp_start_job_run / sp_end_job_run for steps 2, 3, 4...

-- At the END of the Taskflow — SUCCESS path:
CALL sp_end_pipeline_run(
    7,          -- v_pipeline_run_id
    'SUCCESS',
    NULL, NULL, NULL,
    'All steps completed normally'
);

-- At the END of the Taskflow — FAILURE path (e.g. step 3 failed):
CALL sp_end_pipeline_run(
    7,
    'FAILED',
    'LOAD_GL_FACT',   -- which job failed
    3,                -- which step number
    'General ledger mapping failed: source connection timeout',
    NULL
);


-- ── MONITORING QUERIES ──────────────────────────────────────────────────────

-- Pipeline health at a glance
SELECT PIPELINE_NAME, LAST_RUN_START, SUCCESS_RATE_PCT,
       JOBS_TOTAL, AVG_DURATION_SECONDS
FROM VW_PIPELINE_RUN_SUMMARY;

-- All pipeline runs today
SELECT PIPELINE_NAME, STATUS, START_TIME, DURATION_SECONDS,
       JOBS_COMPLETED, JOBS_FAILED, TOTAL_ROWS_INSERTED
FROM VW_PIPELINE_RUN_HISTORY
WHERE START_TIME >= CURRENT_DATE();

-- Step-by-step breakdown of a specific pipeline run
SELECT STEP_NUMBER, JOB_NAME, JOB_STATUS, JOB_DURATION_SECONDS,
       ROWS_INSERTED, ROWS_REJECTED, IS_OPTIONAL
FROM VW_PIPELINE_JOB_BREAKDOWN
WHERE PIPELINE_RUN_ID = 7
ORDER BY STEP_NUMBER;

-- Which pipelines contain a specific job?
SELECT PIPELINE_NAME, STEP_ORDER, IS_OPTIONAL
FROM VW_PIPELINE_DEFINITION
WHERE JOB_NAME = 'LOAD_ACCOUNT_DIM';

-- Pipelines currently running
SELECT PIPELINE_RUN_ID, PIPELINE_NAME, START_TIME, DURATION_SECONDS,
       JOBS_COMPLETED, JOBS_TOTAL
FROM VW_PIPELINE_RUN_HISTORY
WHERE STATUS = 'RUNNING'
ORDER BY START_TIME ASC;
