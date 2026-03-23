/*
================================================================================
  SNOWFLAKE ELT AUDIT FRAMEWORK
  Tool-agnostic: works with Informatica, Matillion, dbt, or any ELT tool
  that can call a stored procedure or execute SQL.

  HOW IT WORKS (plain English)
  ─────────────────────────────
  1. Before a pipeline runs, the ELT tool calls sp_start_job_run().
     This opens a "run record" and returns a RUN_ID.
  2. After the pipeline finishes, the tool calls sp_end_job_run(),
     passing back the RUN_ID plus outcome info (rows loaded, status, etc.).
  3. If something goes wrong mid-run, sp_log_error() captures the detail.
  4. Reporting views sit on top of the raw tables so analysts never
     need to know the underlying schema.

  SWITCHING ELT TOOLS (e.g. Informatica → Matillion)
  ───────────────────────────────────────────────────
  Nothing in this schema changes. You only update how/where you call
  the stored procedures inside your new tool. The audit history is
  preserved and comparable across tools because TOOL_NAME is just
  a column value.

  SETUP ORDER
  ───────────
  Run sections in order: 1 → 2 → 3 → 4 → 5
================================================================================
*/


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 1 — DATABASE & SCHEMA SETUP
   Create a dedicated audit schema so it stays separate from your data.
   ───────────────────────────────────────────────────────────────────────────*/

-- Replace ANALYTICS_DB with whatever your Snowflake database is called.
USE DATABASE ANALYTICS_DB;

CREATE SCHEMA IF NOT EXISTS ELT_AUDIT
  COMMENT = 'Tool-agnostic ELT audit framework. Do not store business data here.';

USE SCHEMA ELT_AUDIT;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 2 — CORE TABLES
   Three tables: catalog (what jobs exist), run log (every execution),
   and error log (every failure detail).
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 2a. JOB CATALOG ────────────────────────────────────────────────────────
-- One row per pipeline/job definition. Register a job here before it runs.
-- Think of this as your "master list" of all ELT jobs.

CREATE TABLE IF NOT EXISTS ELT_JOB_CATALOG (
    JOB_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    JOB_NAME        VARCHAR(200)  NOT NULL,   -- e.g. 'LOAD_SALES_FACT_DAILY'
    JOB_DESCRIPTION VARCHAR(1000),
    SOURCE_SYSTEM   VARCHAR(200),             -- e.g. 'Salesforce', 'Oracle ERP'
    TARGET_SCHEMA   VARCHAR(200),             -- e.g. 'SALES_DW'
    TARGET_TABLE    VARCHAR(200),             -- e.g. 'FACT_SALES'
    JOB_CATEGORY    VARCHAR(100),             -- e.g. 'FULL_LOAD', 'INCREMENTAL', 'CDC'
    OWNER_TEAM      VARCHAR(200),             -- team responsible for this job
    IS_ACTIVE       BOOLEAN DEFAULT TRUE,
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT uq_job_name UNIQUE (JOB_NAME)
)
COMMENT = 'Master catalog of all registered ELT jobs.';


-- ── 2b. JOB RUN LOG ────────────────────────────────────────────────────────
-- One row per execution attempt. This is the core audit trail.
-- TOOL_NAME is what makes the framework tool-agnostic — it is just a value.

CREATE TABLE IF NOT EXISTS ELT_JOB_RUN_LOG (
    RUN_ID              NUMBER AUTOINCREMENT PRIMARY KEY,
    JOB_ID              NUMBER        NOT NULL REFERENCES ELT_JOB_CATALOG(JOB_ID),
    JOB_NAME            VARCHAR(200)  NOT NULL,  -- denormalised for easy querying
    TOOL_NAME           VARCHAR(100)  NOT NULL,  -- 'INFORMATICA' | 'MATILLION' | 'DBT' etc.
    TOOL_JOB_ID         VARCHAR(500),            -- the ID/name inside the ELT tool itself
    ENVIRONMENT         VARCHAR(50)   DEFAULT 'PROD',  -- PROD, UAT, DEV

    -- Timing
    START_TIME          TIMESTAMP_NTZ NOT NULL,
    END_TIME            TIMESTAMP_NTZ,
    DURATION_SECONDS    NUMBER AS (
                            DATEDIFF('second', START_TIME, COALESCE(END_TIME, CURRENT_TIMESTAMP()))
                        ),  -- computed automatically

    -- Outcome
    STATUS              VARCHAR(20)   NOT NULL    -- RUNNING | SUCCESS | FAILED | WARNING
                            CHECK (STATUS IN ('RUNNING','SUCCESS','FAILED','WARNING')),
    ROWS_EXTRACTED      NUMBER DEFAULT 0,
    ROWS_INSERTED       NUMBER DEFAULT 0,
    ROWS_UPDATED        NUMBER DEFAULT 0,
    ROWS_DELETED        NUMBER DEFAULT 0,
    ROWS_REJECTED       NUMBER DEFAULT 0,

    -- Optional load window (useful for incremental/CDC jobs)
    LOAD_WINDOW_START   TIMESTAMP_NTZ,
    LOAD_WINDOW_END     TIMESTAMP_NTZ,

    -- Error summary (full detail goes to ELT_ERROR_LOG)
    ERROR_MESSAGE       VARCHAR(4000),
    ERROR_CODE          VARCHAR(100),

    -- Who triggered the run
    TRIGGERED_BY        VARCHAR(200),  -- service account, user, or scheduler name
    NOTES               VARCHAR(2000),

    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Every execution of every ELT job. One row per run attempt.'
CLUSTER BY (JOB_ID, START_TIME);  -- speeds up queries that filter by job + date


-- ── 2c. ERROR LOG ──────────────────────────────────────────────────────────
-- Detailed error capture. One run can have multiple errors.

CREATE TABLE IF NOT EXISTS ELT_ERROR_LOG (
    ERROR_LOG_ID    NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID          NUMBER        NOT NULL REFERENCES ELT_JOB_RUN_LOG(RUN_ID),
    JOB_NAME        VARCHAR(200)  NOT NULL,  -- denormalised for easy querying
    ERROR_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ERROR_SEVERITY  VARCHAR(20)   DEFAULT 'ERROR'
                        CHECK (ERROR_SEVERITY IN ('INFO','WARNING','ERROR','CRITICAL')),
    ERROR_CODE      VARCHAR(100),
    ERROR_MESSAGE   VARCHAR(4000) NOT NULL,
    ERROR_DETAIL    VARCHAR(8000),           -- stack trace, full context
    SOURCE_RECORD   VARIANT,                 -- the actual row that caused the error (JSON)
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Detailed error records linked to job run log entries.';


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 3 — STORED PROCEDURES
   These are the four procedures your ELT tools call.
   Your tool only needs to know these names — nothing else about the schema.
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 3a. REGISTER A JOB ─────────────────────────────────────────────────────
-- Call once when setting up a new pipeline. Idempotent — safe to call again.

CREATE OR REPLACE PROCEDURE sp_register_job(
    p_job_name        VARCHAR,
    p_description     VARCHAR,
    p_source_system   VARCHAR,
    p_target_schema   VARCHAR,
    p_target_table    VARCHAR,
    p_job_category    VARCHAR,
    p_owner_team      VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO ELT_JOB_CATALOG AS target
    USING (
        SELECT
            :p_job_name       AS JOB_NAME,
            :p_description    AS JOB_DESCRIPTION,
            :p_source_system  AS SOURCE_SYSTEM,
            :p_target_schema  AS TARGET_SCHEMA,
            :p_target_table   AS TARGET_TABLE,
            :p_job_category   AS JOB_CATEGORY,
            :p_owner_team     AS OWNER_TEAM
    ) AS source ON target.JOB_NAME = source.JOB_NAME
    WHEN MATCHED THEN UPDATE SET
        JOB_DESCRIPTION = source.JOB_DESCRIPTION,
        SOURCE_SYSTEM   = source.SOURCE_SYSTEM,
        TARGET_SCHEMA   = source.TARGET_SCHEMA,
        TARGET_TABLE    = source.TARGET_TABLE,
        JOB_CATEGORY    = source.JOB_CATEGORY,
        OWNER_TEAM      = source.OWNER_TEAM,
        UPDATED_AT      = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        JOB_NAME, JOB_DESCRIPTION, SOURCE_SYSTEM,
        TARGET_SCHEMA, TARGET_TABLE, JOB_CATEGORY, OWNER_TEAM
    ) VALUES (
        source.JOB_NAME, source.JOB_DESCRIPTION, source.SOURCE_SYSTEM,
        source.TARGET_SCHEMA, source.TARGET_TABLE, source.JOB_CATEGORY, source.OWNER_TEAM
    );

    RETURN 'Job registered: ' || :p_job_name;
END;
$$
COMMENT = 'Register or update a job in the catalog. Safe to call multiple times.';


-- ── 3b. START A JOB RUN ────────────────────────────────────────────────────
-- Call this at the BEGINNING of every pipeline execution.
-- Returns the RUN_ID — store this and pass it to sp_end_job_run().

CREATE OR REPLACE PROCEDURE sp_start_job_run(
    p_job_name        VARCHAR,
    p_tool_name       VARCHAR,    -- 'INFORMATICA', 'MATILLION', 'DBT', etc.
    p_tool_job_id     VARCHAR,    -- the job/session ID from inside your ELT tool
    p_environment     VARCHAR,    -- 'PROD', 'UAT', 'DEV'
    p_triggered_by    VARCHAR,    -- service account or user name
    p_load_window_start TIMESTAMP_NTZ DEFAULT NULL,
    p_load_window_end   TIMESTAMP_NTZ DEFAULT NULL
)
RETURNS NUMBER  -- returns the new RUN_ID
LANGUAGE SQL
AS
$$
DECLARE
    v_job_id NUMBER;
    v_run_id NUMBER;
BEGIN
    -- Look up the job (raises an error if not registered)
    SELECT JOB_ID INTO v_job_id
    FROM ELT_JOB_CATALOG
    WHERE JOB_NAME = :p_job_name AND IS_ACTIVE = TRUE;

    IF (v_job_id IS NULL) THEN
        RAISE EXCEPTION 'Job not found in catalog: %. Run sp_register_job first.', :p_job_name;
    END IF;

    -- Insert the run record with status RUNNING
    INSERT INTO ELT_JOB_RUN_LOG (
        JOB_ID, JOB_NAME, TOOL_NAME, TOOL_JOB_ID,
        ENVIRONMENT, START_TIME, STATUS,
        TRIGGERED_BY, LOAD_WINDOW_START, LOAD_WINDOW_END
    )
    VALUES (
        v_job_id, :p_job_name, UPPER(:p_tool_name), :p_tool_job_id,
        UPPER(:p_environment), CURRENT_TIMESTAMP(), 'RUNNING',
        :p_triggered_by, :p_load_window_start, :p_load_window_end
    );

    -- Return the new RUN_ID so the caller can pass it to sp_end_job_run
    SELECT MAX(RUN_ID) INTO v_run_id
    FROM ELT_JOB_RUN_LOG
    WHERE JOB_NAME = :p_job_name
      AND STATUS = 'RUNNING'
      AND TRIGGERED_BY = :p_triggered_by;

    RETURN v_run_id;
END;
$$
COMMENT = 'Opens a job run record. Returns RUN_ID — store this for sp_end_job_run.';


-- ── 3c. END A JOB RUN ──────────────────────────────────────────────────────
-- Call this at the END of every pipeline execution (success or failure).

CREATE OR REPLACE PROCEDURE sp_end_job_run(
    p_run_id          NUMBER,
    p_status          VARCHAR,    -- 'SUCCESS', 'FAILED', 'WARNING'
    p_rows_extracted  NUMBER DEFAULT 0,
    p_rows_inserted   NUMBER DEFAULT 0,
    p_rows_updated    NUMBER DEFAULT 0,
    p_rows_deleted    NUMBER DEFAULT 0,
    p_rows_rejected   NUMBER DEFAULT 0,
    p_error_message   VARCHAR DEFAULT NULL,
    p_error_code      VARCHAR DEFAULT NULL,
    p_notes           VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE ELT_JOB_RUN_LOG
    SET
        END_TIME        = CURRENT_TIMESTAMP(),
        STATUS          = UPPER(:p_status),
        ROWS_EXTRACTED  = :p_rows_extracted,
        ROWS_INSERTED   = :p_rows_inserted,
        ROWS_UPDATED    = :p_rows_updated,
        ROWS_DELETED    = :p_rows_deleted,
        ROWS_REJECTED   = :p_rows_rejected,
        ERROR_MESSAGE   = :p_error_message,
        ERROR_CODE      = :p_error_code,
        NOTES           = :p_notes
    WHERE RUN_ID = :p_run_id;

    RETURN 'Run ' || :p_run_id || ' closed with status: ' || UPPER(:p_status);
END;
$$
COMMENT = 'Closes a job run record with outcome details.';


-- ── 3d. LOG AN ERROR ───────────────────────────────────────────────────────
-- Call inside a job when you catch an error. Can be called multiple times.

CREATE OR REPLACE PROCEDURE sp_log_error(
    p_run_id          NUMBER,
    p_job_name        VARCHAR,
    p_error_message   VARCHAR,
    p_error_detail    VARCHAR DEFAULT NULL,
    p_error_code      VARCHAR DEFAULT NULL,
    p_error_severity  VARCHAR DEFAULT 'ERROR',
    p_source_record   VARIANT DEFAULT NULL   -- pass the bad row as JSON if available
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO ELT_ERROR_LOG (
        RUN_ID, JOB_NAME, ERROR_SEVERITY,
        ERROR_CODE, ERROR_MESSAGE, ERROR_DETAIL, SOURCE_RECORD
    )
    VALUES (
        :p_run_id, :p_job_name, UPPER(:p_error_severity),
        :p_error_code, :p_error_message, :p_error_detail, :p_source_record
    );

    RETURN 'Error logged for run ' || :p_run_id;
END;
$$
COMMENT = 'Log an error against an active job run. Call multiple times if needed.';


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 4 — REPORTING VIEWS
   Always query these views — never hit the raw tables directly.
   This protects you if the underlying table structure ever changes.
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 4a. JOB SUMMARY ────────────────────────────────────────────────────────
-- High-level dashboard: last run status, success rate, total rows loaded.

CREATE OR REPLACE VIEW VW_JOB_SUMMARY AS
SELECT
    c.JOB_NAME,
    c.SOURCE_SYSTEM,
    c.TARGET_SCHEMA || '.' || c.TARGET_TABLE   AS TARGET_TABLE_FULL,
    c.JOB_CATEGORY,
    c.OWNER_TEAM,
    COUNT(r.RUN_ID)                             AS TOTAL_RUNS,
    SUM(CASE WHEN r.STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESSFUL_RUNS,
    SUM(CASE WHEN r.STATUS = 'FAILED'  THEN 1 ELSE 0 END) AS FAILED_RUNS,
    ROUND(
        SUM(CASE WHEN r.STATUS = 'SUCCESS' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(r.RUN_ID), 0) * 100, 1
    )                                           AS SUCCESS_RATE_PCT,
    MAX(r.START_TIME)                           AS LAST_RUN_START,
    MAX(CASE WHEN r.STATUS = 'SUCCESS' THEN r.END_TIME END) AS LAST_SUCCESSFUL_RUN,
    SUM(r.ROWS_INSERTED)                        AS TOTAL_ROWS_INSERTED,
    SUM(r.ROWS_UPDATED)                         AS TOTAL_ROWS_UPDATED,
    AVG(r.DURATION_SECONDS)                     AS AVG_DURATION_SECONDS
FROM ELT_JOB_CATALOG c
LEFT JOIN ELT_JOB_RUN_LOG r ON c.JOB_ID = r.JOB_ID
WHERE c.IS_ACTIVE = TRUE
GROUP BY 1,2,3,4,5
ORDER BY c.JOB_NAME;


-- ── 4b. FAILED RUNS ────────────────────────────────────────────────────────
-- Quick view of every failure with its error message — your first stop when
-- something goes wrong.

CREATE OR REPLACE VIEW VW_FAILED_RUNS AS
SELECT
    r.RUN_ID,
    r.JOB_NAME,
    r.TOOL_NAME,
    r.ENVIRONMENT,
    r.START_TIME,
    r.END_TIME,
    r.DURATION_SECONDS,
    r.ERROR_MESSAGE,
    r.ERROR_CODE,
    r.TRIGGERED_BY,
    e.ERROR_SEVERITY,
    e.ERROR_DETAIL
FROM ELT_JOB_RUN_LOG r
LEFT JOIN ELT_ERROR_LOG e ON r.RUN_ID = e.RUN_ID
WHERE r.STATUS = 'FAILED'
ORDER BY r.START_TIME DESC;


-- ── 4c. RECENT RUN HISTORY ─────────────────────────────────────────────────
-- Last 30 days of all runs — useful for trend analysis.

CREATE OR REPLACE VIEW VW_RECENT_RUN_HISTORY AS
SELECT
    r.RUN_ID,
    r.JOB_NAME,
    c.SOURCE_SYSTEM,
    c.TARGET_SCHEMA || '.' || c.TARGET_TABLE AS TARGET_TABLE_FULL,
    r.TOOL_NAME,
    r.ENVIRONMENT,
    r.STATUS,
    r.START_TIME,
    r.END_TIME,
    r.DURATION_SECONDS,
    r.ROWS_EXTRACTED,
    r.ROWS_INSERTED,
    r.ROWS_UPDATED,
    r.ROWS_DELETED,
    r.ROWS_REJECTED,
    r.LOAD_WINDOW_START,
    r.LOAD_WINDOW_END,
    r.TRIGGERED_BY,
    r.NOTES
FROM ELT_JOB_RUN_LOG r
JOIN ELT_JOB_CATALOG c ON r.JOB_ID = c.JOB_ID
WHERE r.START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY r.START_TIME DESC;


-- ── 4d. ROW COUNT ANOMALIES ────────────────────────────────────────────────
-- Flags runs where row counts deviate significantly from the job's average.
-- Useful for catching truncated or over-loaded runs automatically.

CREATE OR REPLACE VIEW VW_ROW_COUNT_ANOMALIES AS
WITH job_stats AS (
    SELECT
        JOB_NAME,
        AVG(ROWS_INSERTED)   AS avg_rows,
        STDDEV(ROWS_INSERTED) AS stddev_rows
    FROM ELT_JOB_RUN_LOG
    WHERE STATUS = 'SUCCESS' AND ROWS_INSERTED > 0
    GROUP BY JOB_NAME
    HAVING COUNT(*) >= 5  -- only flag when we have enough history
)
SELECT
    r.RUN_ID,
    r.JOB_NAME,
    r.START_TIME,
    r.ROWS_INSERTED,
    ROUND(s.avg_rows, 0)     AS historical_avg_rows,
    ROUND(s.stddev_rows, 0)  AS historical_stddev,
    ROUND(
        (r.ROWS_INSERTED - s.avg_rows) / NULLIF(s.stddev_rows, 0), 2
    )                        AS z_score,  -- how many std deviations from normal
    CASE
        WHEN ABS((r.ROWS_INSERTED - s.avg_rows) / NULLIF(s.stddev_rows, 0)) > 3
        THEN 'CRITICAL ANOMALY'
        WHEN ABS((r.ROWS_INSERTED - s.avg_rows) / NULLIF(s.stddev_rows, 0)) > 2
        THEN 'WARNING'
        ELSE 'NORMAL'
    END                      AS anomaly_flag
FROM ELT_JOB_RUN_LOG r
JOIN job_stats s ON r.JOB_NAME = s.JOB_NAME
WHERE r.STATUS = 'SUCCESS'
  AND ABS((r.ROWS_INSERTED - s.avg_rows) / NULLIF(s.stddev_rows, 0)) > 2
ORDER BY ABS((r.ROWS_INSERTED - s.avg_rows) / NULLIF(s.stddev_rows, 0)) DESC;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 5 — EXAMPLE USAGE
   Run these manually to test the framework is working.
   ───────────────────────────────────────────────────────────────────────────*/

-- Step 1: Register a job (do this once per pipeline)
CALL sp_register_job(
    'LOAD_SALES_FACT_DAILY',
    'Loads daily sales transactions from Salesforce into FACT_SALES',
    'Salesforce',
    'SALES_DW',
    'FACT_SALES',
    'INCREMENTAL',
    'Data Engineering'
);

-- Step 2: Start a run (do this at the beginning of each execution)
-- Note: store the returned RUN_ID in a variable inside your ELT tool
CALL sp_start_job_run(
    'LOAD_SALES_FACT_DAILY',  -- job name (must match catalog)
    'INFORMATICA',            -- your ELT tool name
    'WF_LOAD_SALES_20240115', -- the workflow/session ID from your tool
    'PROD',                   -- environment
    'svc_informatica',        -- service account
    '2024-01-14 00:00:00'::TIMESTAMP_NTZ,  -- load window start
    '2024-01-15 00:00:00'::TIMESTAMP_NTZ   -- load window end
);

-- Step 3a: End a SUCCESSFUL run
-- (Replace 1 with the actual RUN_ID returned from sp_start_job_run)
CALL sp_end_job_run(
    1,          -- RUN_ID from step 2
    'SUCCESS',
    50000,      -- rows extracted from source
    48500,      -- rows inserted
    1200,       -- rows updated
    0,          -- rows deleted
    300,        -- rows rejected
    NULL,       -- no error
    NULL,
    'Normal daily load completed'
);

-- Step 3b: End a FAILED run (alternative to 3a)
CALL sp_end_job_run(
    2,          -- RUN_ID
    'FAILED',
    50000,      -- rows extracted before failure
    0, 0, 0, 0,
    'Connection timeout to source system after 30 minutes',
    'ORA-12170',
    NULL
);

-- Optional: Log additional error detail
CALL sp_log_error(
    2,           -- RUN_ID
    'LOAD_SALES_FACT_DAILY',
    'Connection timeout to source system',
    'Attempted to connect to Salesforce API. Connection dropped at 30m mark. Batch ID: 78234.',
    'ORA-12170',
    'CRITICAL'
);

-- Query the views to see results
SELECT * FROM VW_JOB_SUMMARY;
SELECT * FROM VW_FAILED_RUNS;
SELECT * FROM VW_RECENT_RUN_HISTORY LIMIT 20;
SELECT * FROM VW_ROW_COUNT_ANOMALIES;


/*
================================================================================
  HOW TO INTEGRATE WITH YOUR ELT TOOL

  INFORMATICA (current):
    In your mapping or workflow, add a Pre-Session command:
      CALL ELT_AUDIT.sp_start_job_run('MY_JOB', 'INFORMATICA', ...)
    Capture the returned RUN_ID using a workflow variable.
    In Post-Session Success:
      CALL ELT_AUDIT.sp_end_job_run($RUN_ID, 'SUCCESS', ...)
    In Post-Session Failure:
      CALL ELT_AUDIT.sp_end_job_run($RUN_ID, 'FAILED', 0,0,0,0,0, $$Error message$$, ...)

  MATILLION (future):
    In your job, add a Snowflake Query component at the start:
      CALL ELT_AUDIT.sp_start_job_run('MY_JOB', 'MATILLION', ...)
    Store the result in a job variable (e.g. v_run_id).
    At the end of the job, add another Snowflake Query component:
      CALL ELT_AUDIT.sp_end_job_run(${v_run_id}, 'SUCCESS', ...)
    Use an exception handler component for the failure path.

  DBT:
    Use dbt's on-run-start and on-run-end hooks in dbt_project.yml:
      on-run-start:
        - "CALL ELT_AUDIT.sp_start_job_run('{{ invocation_id }}', 'DBT', ...)"

  KEY POINT: The stored procedure names never change.
  Only the TOOL_NAME value changes when you switch tools.
================================================================================
*/
