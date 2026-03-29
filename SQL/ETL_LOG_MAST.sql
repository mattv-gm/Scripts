/*
================================================================================
  SNOWFLAKE ELT AUDIT FRAMEWORK — MASTER DEPLOYMENT SCRIPT
  Version 1.0  |  March 2026

  WHAT THIS SCRIPT DOES
  ─────────────────────
  Deploys the complete ELT Audit Framework in a single run:

    MODULE 1 — Core framework
                Tables:      ELT_JOB_CATALOG, ELT_JOB_RUN_LOG, ELT_ERROR_LOG
                Procedures:  sp_register_job, sp_start_job_run, sp_end_job_run,
                             sp_log_error
                Views:       VW_JOB_SUMMARY, VW_FAILED_RUNS,
                             VW_RECENT_RUN_HISTORY, VW_ROW_COUNT_ANOMALIES

    MODULE 2 — File tracking extension
                Tables:      ELT_FILE_LOG
                Procedures:  sp_start_file, sp_end_file, sp_register_file_batch
                Views:       VW_FILE_HISTORY, VW_FILE_FAILURES,
                             VW_UNPROCESSED_FILES, VW_DUPLICATE_FILES,
                             VW_FILE_PROCESSING_SUMMARY

    MODULE 3 — Taskflow orchestration extension
                Tables:      ELT_TASKFLOW_CATALOG, ELT_TASKFLOW_JOBS,
                             ELT_TASKFLOW_RUN
                             + TASKFLOW_RUN_ID column added to ELT_JOB_RUN_LOG
                Procedures:  sp_register_taskflow, sp_register_taskflow_job,
                             sp_start_taskflow_run, sp_close_taskflow_run
                             + sp_start_job_run updated (backwards compatible)
                Views:       VW_TASKFLOW_SUMMARY, VW_TASKFLOW_RUN_HISTORY,
                             VW_TASKFLOW_JOB_DETAIL, VW_TASKFLOW_FAILURES,
                             VW_TASKFLOW_STRUCTURE

    MODULE 4 — Permissions
                Roles:       ELT_ROLE (service account), ELT_AUDIT_READER
                             (analysts / BI tools)

  BEFORE YOU RUN
  ──────────────
  1. Replace ANALYTICS_DB with your actual Snowflake database name.
     Use Find & Replace — it appears throughout the script.
  2. Replace ELT_WH with your actual warehouse name.
  3. Replace SVC_INFORMATICA with your actual service account username.
  4. Run as SYSADMIN (for objects) then SECURITYADMIN (for permissions).
     The script switches roles automatically where needed.

  RUN ORDER
  ─────────
  Run this entire script top to bottom in a single Snowflake worksheet.
  Each module is idempotent — safe to re-run if something fails partway through.

  FULL DATA MODEL (parent → child)
  ─────────────────────────────────
  ELT_TASKFLOW_CATALOG
    └── ELT_TASKFLOW_RUN          (1 row per Taskflow execution)
          └── ELT_JOB_RUN_LOG     (1 row per job within the Taskflow)
                ├── ELT_ERROR_LOG (1+ rows per job failure)
                └── ELT_FILE_LOG  (1 row per file processed)

  Standalone jobs (not part of a Taskflow) go directly into ELT_JOB_RUN_LOG
  with TASKFLOW_RUN_ID = NULL.
================================================================================
*/

USE ROLE SYSADMIN;


/* ════════════════════════════════════════════════════════════════════════════
   MODULE 1 — CORE FRAMEWORK
   ════════════════════════════════════════════════════════════════════════════*/
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
    -- Valid values: RUNNING | SUCCESS | FAILED | WARNING
    -- Enforced in sp_start_job_run() and sp_end_job_run() — not a CHECK constraint
    -- (Snowflake does not support CHECK constraints)
    STATUS              VARCHAR(20)   NOT NULL,
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
    ERROR_SEVERITY  VARCHAR(20)   DEFAULT 'ERROR',  -- INFO | WARNING | ERROR | CRITICAL
                                                     -- Enforced in sp_log_error()
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
    -- Validate status value (replaces CHECK constraint not supported by Snowflake)
    IF (UPPER(:p_status) NOT IN ('SUCCESS', 'FAILED', 'WARNING')) THEN
        RAISE EXCEPTION 'Invalid status "%". Must be one of: SUCCESS, FAILED, WARNING.', :p_status;
    END IF;

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
    -- Validate severity value (replaces CHECK constraint not supported by Snowflake)
    IF (UPPER(:p_error_severity) NOT IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')) THEN
        RAISE EXCEPTION 'Invalid severity "%". Must be one of: INFO, WARNING, ERROR, CRITICAL.', :p_error_severity;
    END IF;

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




/* ════════════════════════════════════════════════════════════════════════════
   MODULE 2 — FILE TRACKING EXTENSION
   ════════════════════════════════════════════════════════════════════════════*/
/*
  PURPOSE: Adds ELT_FILE_LOG to track individual files processed by
  file-based pipelines (CSV, JSON, Parquet, etc.).
  Each file gets one record per run attempt, linked to its parent job run.
*/
USE DATABASE ANALYTICS_DB;
USE SCHEMA ELT_AUDIT;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 1 — FILE LOG TABLE
   One row per file per run. Captures the full lifecycle of each file from
   detection through to completion or failure.
   ───────────────────────────────────────────────────────────────────────────*/

CREATE TABLE IF NOT EXISTS ELT_FILE_LOG (

    -- Identity
    FILE_LOG_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID              NUMBER        NOT NULL REFERENCES ELT_JOB_RUN_LOG(RUN_ID),
    JOB_NAME            VARCHAR(200)  NOT NULL,   -- denormalised for easy querying

    -- File identity
    FILE_NAME           VARCHAR(500)  NOT NULL,   -- e.g. 'SALES_20240115_001.csv'
    FILE_PATH           VARCHAR(2000),            -- full path or S3/ADLS URI
    FILE_SOURCE_SYSTEM  VARCHAR(200),             -- e.g. 'S3', 'SFTP', 'SharePoint', 'local'
    FILE_TYPE           VARCHAR(50),              -- e.g. 'CSV', 'JSON', 'PARQUET', 'XML', 'FIXED'

    -- File metadata (captured at detection time)
    FILE_SIZE_BYTES     NUMBER,
    FILE_CHECKSUM       VARCHAR(200),             -- MD5 or SHA256 — use for duplicate detection
    FILE_LAST_MODIFIED  TIMESTAMP_NTZ,           -- last modified timestamp on the source file
    FILE_DETECTED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),  -- when the pipeline saw it

    -- Processing timeline
    PROCESSING_START    TIMESTAMP_NTZ,           -- when the pipeline began reading this file
    PROCESSING_END      TIMESTAMP_NTZ,           -- when processing finished (success or fail)
    PROCESSING_DURATION_SECONDS NUMBER AS (
                            DATEDIFF('second',
                                PROCESSING_START,
                                COALESCE(PROCESSING_END, CURRENT_TIMESTAMP())
                            )
                        ),

    -- Outcome
    -- Valid values: DETECTED | IN_PROGRESS | LOADED | PARTIAL | FAILED | SKIPPED | DUPLICATE
    -- Enforced in sp_start_file() and sp_end_file() — not a CHECK constraint
    -- (Snowflake does not support CHECK constraints)
    FILE_STATUS         VARCHAR(30)   NOT NULL,

    -- Row-level statistics
    ROWS_IN_FILE        NUMBER DEFAULT 0,        -- total rows in the file (inc. header if applicable)
    ROWS_ATTEMPTED      NUMBER DEFAULT 0,        -- rows the pipeline tried to process
    ROWS_LOADED         NUMBER DEFAULT 0,        -- rows successfully written to target
    ROWS_REJECTED       NUMBER DEFAULT 0,        -- rows that failed validation/transformation
    ROWS_DUPLICATE      NUMBER DEFAULT 0,        -- rows skipped due to duplicate key detection

    -- Failure detail
    REJECT_REASON       VARCHAR(2000),           -- high-level reason for FAILED/PARTIAL status
    REJECT_FILE_PATH    VARCHAR(2000),           -- path to the reject/bad file if the tool creates one

    -- Audit trail
    TARGET_TABLE        VARCHAR(500),            -- which table this file loaded into
    LOAD_BATCH_ID       VARCHAR(200),            -- optional batch/group ID if files are batched together
    NOTES               VARCHAR(2000),

    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'One row per file per run. Full lifecycle tracking from detection to completion.'
CLUSTER BY (RUN_ID, FILE_DETECTED_AT);


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 2 — STORED PROCEDURES
   Two procedures: one to open a file record, one to close it.
   Your ELT tool calls these around each individual file's processing.
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 2a. LOG FILE START ─────────────────────────────────────────────────────
-- Call when the pipeline begins processing a single file.
-- Returns the FILE_LOG_ID — store this and pass it to sp_end_file().
-- Call AFTER sp_start_job_run() so you have a valid RUN_ID to link to.

CREATE OR REPLACE PROCEDURE sp_start_file(
    p_run_id            NUMBER,
    p_job_name          VARCHAR,
    p_file_name         VARCHAR,
    p_file_path         VARCHAR         DEFAULT NULL,
    p_file_source       VARCHAR         DEFAULT NULL,   -- 'S3', 'SFTP', etc.
    p_file_type         VARCHAR         DEFAULT NULL,   -- 'CSV', 'JSON', etc.
    p_file_size_bytes   NUMBER          DEFAULT NULL,
    p_file_checksum     VARCHAR         DEFAULT NULL,
    p_file_last_modified TIMESTAMP_NTZ  DEFAULT NULL,
    p_rows_in_file      NUMBER          DEFAULT 0,
    p_target_table      VARCHAR         DEFAULT NULL,
    p_load_batch_id     VARCHAR         DEFAULT NULL
)
RETURNS NUMBER   -- returns FILE_LOG_ID
LANGUAGE SQL
AS
$$
DECLARE
    v_file_log_id NUMBER;
    v_existing_id NUMBER;
    v_status      VARCHAR;
BEGIN
    -- Duplicate detection: check if this exact file (by checksum) was
    -- successfully loaded before by ANY prior run of this job.
    IF (:p_file_checksum IS NOT NULL) THEN
        SELECT FILE_LOG_ID INTO v_existing_id
        FROM ELT_FILE_LOG
        WHERE JOB_NAME      = :p_job_name
          AND FILE_CHECKSUM = :p_file_checksum
          AND FILE_STATUS   = 'LOADED'
        ORDER BY PROCESSING_END DESC
        LIMIT 1;
    END IF;

    -- Set initial status: DUPLICATE if we've seen this checksum before,
    -- otherwise IN_PROGRESS.
    v_status := CASE WHEN v_existing_id IS NOT NULL THEN 'DUPLICATE' ELSE 'IN_PROGRESS' END;

    INSERT INTO ELT_FILE_LOG (
        RUN_ID, JOB_NAME, FILE_NAME, FILE_PATH,
        FILE_SOURCE_SYSTEM, FILE_TYPE,
        FILE_SIZE_BYTES, FILE_CHECKSUM, FILE_LAST_MODIFIED,
        FILE_DETECTED_AT, PROCESSING_START,
        FILE_STATUS, ROWS_IN_FILE,
        TARGET_TABLE, LOAD_BATCH_ID,
        NOTES
    )
    VALUES (
        :p_run_id, :p_job_name, :p_file_name, :p_file_path,
        :p_file_source, :p_file_type,
        :p_file_size_bytes, :p_file_checksum, :p_file_last_modified,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
        v_status, :p_rows_in_file,
        :p_target_table, :p_load_batch_id,
        CASE WHEN v_existing_id IS NOT NULL
             THEN 'Duplicate of FILE_LOG_ID ' || v_existing_id
             ELSE NULL END
    );

    SELECT MAX(FILE_LOG_ID) INTO v_file_log_id
    FROM ELT_FILE_LOG
    WHERE RUN_ID   = :p_run_id
      AND FILE_NAME = :p_file_name
      AND PROCESSING_START >= DATEADD('minute', -1, CURRENT_TIMESTAMP());

    RETURN v_file_log_id;
END;
$$
COMMENT = 'Opens a file record. Returns FILE_LOG_ID. Call sp_end_file() when processing completes.';


-- ── 2b. LOG FILE END ───────────────────────────────────────────────────────
-- Call when the pipeline finishes processing a single file — whether it
-- succeeded, partially loaded, or failed completely.

CREATE OR REPLACE PROCEDURE sp_end_file(
    p_file_log_id       NUMBER,
    p_file_status       VARCHAR,        -- 'LOADED', 'PARTIAL', 'FAILED', 'SKIPPED', 'DUPLICATE'
    p_rows_attempted    NUMBER  DEFAULT 0,
    p_rows_loaded       NUMBER  DEFAULT 0,
    p_rows_rejected     NUMBER  DEFAULT 0,
    p_rows_duplicate    NUMBER  DEFAULT 0,
    p_reject_reason     VARCHAR DEFAULT NULL,
    p_reject_file_path  VARCHAR DEFAULT NULL,
    p_notes             VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Validate file status value (replaces CHECK constraint not supported by Snowflake)
    IF (UPPER(:p_file_status) NOT IN ('LOADED', 'PARTIAL', 'FAILED', 'SKIPPED', 'DUPLICATE')) THEN
        RAISE EXCEPTION 'Invalid file status "%". Must be one of: LOADED, PARTIAL, FAILED, SKIPPED, DUPLICATE.', :p_file_status;
    END IF;

    UPDATE ELT_FILE_LOG
    SET
        PROCESSING_END      = CURRENT_TIMESTAMP(),
        FILE_STATUS         = UPPER(:p_file_status),
        ROWS_ATTEMPTED      = :p_rows_attempted,
        ROWS_LOADED         = :p_rows_loaded,
        ROWS_REJECTED       = :p_rows_rejected,
        ROWS_DUPLICATE      = :p_rows_duplicate,
        REJECT_REASON       = :p_reject_reason,
        REJECT_FILE_PATH    = :p_reject_file_path,
        NOTES               = :p_notes
    WHERE FILE_LOG_ID = :p_file_log_id;

    RETURN 'File record ' || :p_file_log_id || ' closed with status: ' || UPPER(:p_file_status);
END;
$$
COMMENT = 'Closes a file record with outcome statistics.';


-- ── 2c. BULK REGISTER FILES (convenience) ─────────────────────────────────
-- Use this when you know the full file list before processing starts
-- (e.g. you landed a batch of 50 files in S3 and want to register them
-- all as DETECTED before the pipeline picks them up one by one).

CREATE OR REPLACE PROCEDURE sp_register_file_batch(
    p_run_id        NUMBER,
    p_job_name      VARCHAR,
    p_file_list     VARIANT,        -- pass as JSON array: [{"name":"f1.csv","path":"s3://...","size":1024}, ...]
    p_file_source   VARCHAR DEFAULT NULL,
    p_file_type     VARCHAR DEFAULT NULL,
    p_target_table  VARCHAR DEFAULT NULL,
    p_load_batch_id VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_count NUMBER DEFAULT 0;
BEGIN
    INSERT INTO ELT_FILE_LOG (
        RUN_ID, JOB_NAME, FILE_NAME, FILE_PATH,
        FILE_SOURCE_SYSTEM, FILE_TYPE,
        FILE_SIZE_BYTES, FILE_STATUS,
        TARGET_TABLE, LOAD_BATCH_ID,
        FILE_DETECTED_AT
    )
    SELECT
        :p_run_id,
        :p_job_name,
        f.value:name::VARCHAR,
        f.value:path::VARCHAR,
        COALESCE(f.value:source::VARCHAR, :p_file_source),
        COALESCE(f.value:type::VARCHAR,   :p_file_type),
        f.value:size_bytes::NUMBER,
        'DETECTED',
        :p_target_table,
        :p_load_batch_id,
        CURRENT_TIMESTAMP()
    FROM TABLE(FLATTEN(input => :p_file_list)) f;

    SELECT COUNT(*) INTO v_count
    FROM ELT_FILE_LOG
    WHERE RUN_ID = :p_run_id AND LOAD_BATCH_ID = :p_load_batch_id;

    RETURN v_count || ' files registered as DETECTED for run ' || :p_run_id;
END;
$$
COMMENT = 'Bulk-register a JSON array of files as DETECTED before processing begins.';


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 3 — REPORTING VIEWS
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 3a. FILE PROCESSING SUMMARY ────────────────────────────────────────────
-- Roll-up per job: how many files processed, loaded, failed, duplicated.

CREATE OR REPLACE VIEW VW_FILE_PROCESSING_SUMMARY AS
SELECT
    f.JOB_NAME,
    DATE_TRUNC('day', f.FILE_DETECTED_AT)               AS PROCESS_DATE,
    COUNT(*)                                             AS TOTAL_FILES_SEEN,
    SUM(CASE WHEN f.FILE_STATUS = 'LOADED'    THEN 1 ELSE 0 END) AS FILES_LOADED,
    SUM(CASE WHEN f.FILE_STATUS = 'PARTIAL'   THEN 1 ELSE 0 END) AS FILES_PARTIAL,
    SUM(CASE WHEN f.FILE_STATUS = 'FAILED'    THEN 1 ELSE 0 END) AS FILES_FAILED,
    SUM(CASE WHEN f.FILE_STATUS = 'SKIPPED'   THEN 1 ELSE 0 END) AS FILES_SKIPPED,
    SUM(CASE WHEN f.FILE_STATUS = 'DUPLICATE' THEN 1 ELSE 0 END) AS FILES_DUPLICATE,
    SUM(f.ROWS_IN_FILE)                                  AS TOTAL_ROWS_IN_FILES,
    SUM(f.ROWS_LOADED)                                   AS TOTAL_ROWS_LOADED,
    SUM(f.ROWS_REJECTED)                                 AS TOTAL_ROWS_REJECTED,
    SUM(f.FILE_SIZE_BYTES)                               AS TOTAL_BYTES_PROCESSED,
    ROUND(AVG(f.PROCESSING_DURATION_SECONDS), 1)         AS AVG_FILE_PROCESSING_SECS,
    MAX(f.PROCESSING_DURATION_SECONDS)                   AS MAX_FILE_PROCESSING_SECS
FROM ELT_FILE_LOG f
GROUP BY 1, 2
ORDER BY 1, 2 DESC;


-- ── 3b. FAILED AND PARTIAL FILES ───────────────────────────────────────────
-- Your first stop when a file-based pipeline reports errors.

CREATE OR REPLACE VIEW VW_FILE_FAILURES AS
SELECT
    f.FILE_LOG_ID,
    f.RUN_ID,
    f.JOB_NAME,
    f.FILE_NAME,
    f.FILE_PATH,
    f.FILE_SOURCE_SYSTEM,
    f.FILE_STATUS,
    f.ROWS_IN_FILE,
    f.ROWS_LOADED,
    f.ROWS_REJECTED,
    ROUND(
        f.ROWS_REJECTED / NULLIF(f.ROWS_IN_FILE, 0) * 100, 2
    )                                               AS REJECT_RATE_PCT,
    f.REJECT_REASON,
    f.REJECT_FILE_PATH,
    f.FILE_DETECTED_AT,
    f.PROCESSING_START,
    f.PROCESSING_END,
    f.PROCESSING_DURATION_SECONDS,
    r.TOOL_NAME,
    r.ENVIRONMENT
FROM ELT_FILE_LOG f
JOIN ELT_JOB_RUN_LOG r ON f.RUN_ID = r.RUN_ID
WHERE f.FILE_STATUS IN ('FAILED', 'PARTIAL')
ORDER BY f.FILE_DETECTED_AT DESC;


-- ── 3c. DUPLICATE FILE DETECTION ───────────────────────────────────────────
-- Shows all files that were detected as duplicates. Useful for debugging
-- pipelines that pick up the same file more than once.

CREATE OR REPLACE VIEW VW_DUPLICATE_FILES AS
SELECT
    f.FILE_LOG_ID,
    f.RUN_ID,
    f.JOB_NAME,
    f.FILE_NAME,
    f.FILE_CHECKSUM,
    f.FILE_DETECTED_AT,
    f.NOTES                                AS DUPLICATE_OF,
    -- Also show the original successful load for comparison
    orig.FILE_LOG_ID                       AS ORIGINAL_FILE_LOG_ID,
    orig.RUN_ID                            AS ORIGINAL_RUN_ID,
    orig.PROCESSING_END                    AS ORIGINAL_LOADED_AT,
    orig.ROWS_LOADED                       AS ORIGINAL_ROWS_LOADED
FROM ELT_FILE_LOG f
LEFT JOIN ELT_FILE_LOG orig
    ON  f.JOB_NAME      = orig.JOB_NAME
    AND f.FILE_CHECKSUM = orig.FILE_CHECKSUM
    AND orig.FILE_STATUS = 'LOADED'
WHERE f.FILE_STATUS = 'DUPLICATE'
ORDER BY f.FILE_DETECTED_AT DESC;


-- ── 3d. UNPROCESSED FILES ──────────────────────────────────────────────────
-- Files registered as DETECTED or stuck IN_PROGRESS for more than 2 hours.
-- Helps you spot files that were seen but never processed.

CREATE OR REPLACE VIEW VW_UNPROCESSED_FILES AS
SELECT
    f.FILE_LOG_ID,
    f.RUN_ID,
    f.JOB_NAME,
    f.FILE_NAME,
    f.FILE_PATH,
    f.FILE_STATUS,
    f.FILE_DETECTED_AT,
    DATEDIFF('minute', f.FILE_DETECTED_AT, CURRENT_TIMESTAMP()) AS MINUTES_WAITING,
    CASE
        WHEN f.FILE_STATUS = 'DETECTED'    THEN 'Waiting to be picked up by pipeline'
        WHEN f.FILE_STATUS = 'IN_PROGRESS'
         AND DATEDIFF('hour', f.PROCESSING_START, CURRENT_TIMESTAMP()) > 2
                                           THEN 'WARNING: In progress for over 2 hours — possible hang'
        ELSE 'In progress (normal)'
    END AS DIAGNOSIS
FROM ELT_FILE_LOG f
WHERE f.FILE_STATUS IN ('DETECTED', 'IN_PROGRESS')
ORDER BY f.FILE_DETECTED_AT ASC;


-- ── 3e. FILE HISTORY FOR A SPECIFIC FILE NAME ──────────────────────────────
-- Pass a file name pattern to see every time that file was processed.
-- Useful for investigating "did this file load last week?" type questions.
-- Usage: SELECT * FROM VW_FILE_HISTORY WHERE FILE_NAME LIKE 'SALES_%';

CREATE OR REPLACE VIEW VW_FILE_HISTORY AS
SELECT
    f.FILE_LOG_ID,
    f.RUN_ID,
    f.JOB_NAME,
    f.FILE_NAME,
    f.FILE_PATH,
    f.FILE_STATUS,
    f.FILE_SIZE_BYTES,
    f.ROWS_IN_FILE,
    f.ROWS_LOADED,
    f.ROWS_REJECTED,
    f.FILE_CHECKSUM,
    f.FILE_DETECTED_AT,
    f.PROCESSING_START,
    f.PROCESSING_END,
    f.PROCESSING_DURATION_SECONDS,
    f.REJECT_REASON,
    r.TOOL_NAME,
    r.ENVIRONMENT
FROM ELT_FILE_LOG f
JOIN ELT_JOB_RUN_LOG r ON f.RUN_ID = r.RUN_ID
ORDER BY f.FILE_DETECTED_AT DESC;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 4 — EXAMPLE USAGE
   ───────────────────────────────────────────────────────────────────────────*/

-- ── SCENARIO A: Single file, manual step-by-step ───────────────────────────
-- This is what your ELT tool does for each file it processes.

-- Step 1: Start the job run as normal (from the base framework)
CALL sp_start_job_run(
    'LOAD_SALES_FACT_DAILY',
    'INFORMATICA',
    'WF_LOAD_SALES_20240115',
    'PROD',
    'svc_informatica'
);
-- Store the returned RUN_ID (assume it's 42 here)

-- Step 2: For EACH file the pipeline picks up, open a file record
CALL sp_start_file(
    42,                             -- RUN_ID from step above
    'LOAD_SALES_FACT_DAILY',        -- JOB_NAME
    'SALES_20240115_001.csv',       -- FILE_NAME
    's3://my-bucket/inbound/SALES_20240115_001.csv',  -- FILE_PATH
    'S3',                           -- FILE_SOURCE_SYSTEM
    'CSV',                          -- FILE_TYPE
    204800,                         -- FILE_SIZE_BYTES (200 KB)
    'a3f4b2c1d9e8f7a6b5c4d3e2f1a0', -- FILE_CHECKSUM (MD5)
    '2024-01-15 06:00:00'::TIMESTAMP_NTZ,  -- FILE_LAST_MODIFIED
    5000,                           -- ROWS_IN_FILE
    'SALES_DW.FACT_SALES',          -- TARGET_TABLE
    'BATCH_20240115_AM'             -- LOAD_BATCH_ID
);
-- Store the returned FILE_LOG_ID (assume it's 1 here)

-- Step 3a: Close a SUCCESSFUL file
CALL sp_end_file(
    1,           -- FILE_LOG_ID
    'LOADED',
    5000,        -- rows attempted
    4980,        -- rows loaded
    20,          -- rows rejected
    0,           -- rows duplicate
    '20 rows rejected: invalid date format in SALE_DATE column',
    's3://my-bucket/rejects/SALES_20240115_001_rejects.csv',
    NULL
);

-- Step 3b: Close a FAILED file (alternative)
CALL sp_end_file(
    2,           -- FILE_LOG_ID
    'FAILED',
    0, 0, 0, 0,
    'File header row count mismatch — expected 24 columns, found 22',
    NULL,
    'File may be truncated or from wrong source system'
);

-- Step 4: Close the job run as normal
CALL sp_end_job_run(42, 'SUCCESS', 5000, 4980, 0, 0, 20);


-- ── SCENARIO B: Bulk register then process ─────────────────────────────────
-- Use when you want to register a whole landing zone of files upfront
-- before the pipeline starts picking them up.

-- Register a batch of files detected in S3
CALL sp_register_file_batch(
    42,                             -- RUN_ID
    'LOAD_SALES_FACT_DAILY',
    PARSE_JSON('[
        {"name": "SALES_20240115_001.csv", "path": "s3://bucket/inbound/SALES_20240115_001.csv", "size_bytes": 204800},
        {"name": "SALES_20240115_002.csv", "path": "s3://bucket/inbound/SALES_20240115_002.csv", "size_bytes": 198400},
        {"name": "SALES_20240115_003.csv", "path": "s3://bucket/inbound/SALES_20240115_003.csv", "size_bytes": 210000}
    ]'),
    'S3',
    'CSV',
    'SALES_DW.FACT_SALES',
    'BATCH_20240115_AM'
);
-- Now the pipeline processes each file and calls sp_start_file / sp_end_file
-- for each one individually.


-- ── QUERY EXAMPLES ─────────────────────────────────────────────────────────

-- "Show me every file we tried to load today"
SELECT FILE_NAME, FILE_STATUS, ROWS_LOADED, ROWS_REJECTED, REJECT_REASON
FROM VW_FILE_HISTORY
WHERE FILE_DETECTED_AT >= CURRENT_DATE()
ORDER BY FILE_DETECTED_AT DESC;

-- "Which files failed this week?"
SELECT * FROM VW_FILE_FAILURES
WHERE FILE_DETECTED_AT >= DATEADD('day', -7, CURRENT_DATE());

-- "Have we ever loaded a file called SALES_20240115_001.csv?"
SELECT * FROM VW_FILE_HISTORY
WHERE FILE_NAME = 'SALES_20240115_001.csv';

-- "Are there any files that haven't been processed yet?"
SELECT * FROM VW_UNPROCESSED_FILES;

-- "Show me the file load summary for this month"
SELECT * FROM VW_FILE_PROCESSING_SUMMARY
WHERE PROCESS_DATE >= DATE_TRUNC('month', CURRENT_DATE());

-- "Did we accidentally pick up the same file twice?"
SELECT * FROM VW_DUPLICATE_FILES
WHERE FILE_DETECTED_AT >= CURRENT_DATE();

-- "What's the reject rate per file type?"
SELECT
    FILE_TYPE,
    COUNT(*)                                                 AS FILE_COUNT,
    SUM(ROWS_IN_FILE)                                        AS TOTAL_ROWS,
    SUM(ROWS_REJECTED)                                       AS TOTAL_REJECTED,
    ROUND(SUM(ROWS_REJECTED) / NULLIF(SUM(ROWS_IN_FILE),0) * 100, 2) AS OVERALL_REJECT_PCT
FROM ELT_FILE_LOG
WHERE FILE_STATUS IN ('LOADED', 'PARTIAL')
GROUP BY FILE_TYPE
ORDER BY OVERALL_REJECT_PCT DESC;


/* ════════════════════════════════════════════════════════════════════════════
   MODULE 3 — TASKFLOW ORCHESTRATION EXTENSION
   ════════════════════════════════════════════════════════════════════════════*/
/*
  PURPOSE: Adds a parent layer above ELT_JOB_RUN_LOG to capture the overall
  Taskflow/orchestration that groups multiple job runs. Supports mixed
  sequential and parallel execution, and critical vs non-critical job flagging.
*/
USE DATABASE ANALYTICS_DB;
USE SCHEMA ELT_AUDIT;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 1 — TABLE CHANGES
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 1a. TASKFLOW CATALOG ───────────────────────────────────────────────────
-- One row per Taskflow definition. Mirrors ELT_JOB_CATALOG but at the
-- orchestration level. Register each Taskflow here before its first run.

CREATE TABLE IF NOT EXISTS ELT_TASKFLOW_CATALOG (
    TASKFLOW_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    TASKFLOW_NAME       VARCHAR(200)  NOT NULL,   -- e.g. 'DAILY_SALES_LOAD'
    TASKFLOW_DESC       VARCHAR(1000),
    SCHEDULE            VARCHAR(200),             -- e.g. 'Daily 02:00 UTC', 'Hourly'
    OWNER_TEAM          VARCHAR(200),
    TOOL_NAME           VARCHAR(100),             -- 'IICS', 'MATILLION', etc.
    IS_ACTIVE           BOOLEAN DEFAULT TRUE,
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT uq_taskflow_name UNIQUE (TASKFLOW_NAME)
)
COMMENT = 'Master catalog of all registered Taskflow orchestrations.';


-- ── 1b. TASKFLOW JOB MEMBERSHIP ───────────────────────────────────────────
-- Defines which jobs belong to which Taskflow and how they relate to each
-- other. This is the catalog-level definition — not a runtime record.
-- IS_CRITICAL controls how a job failure affects the overall Taskflow status.
-- EXECUTION_ORDER is for documentation only (parallel jobs share the same value).

CREATE TABLE IF NOT EXISTS ELT_TASKFLOW_JOBS (
    TASKFLOW_JOB_ID     NUMBER AUTOINCREMENT PRIMARY KEY,
    TASKFLOW_ID         NUMBER        NOT NULL REFERENCES ELT_TASKFLOW_CATALOG(TASKFLOW_ID),
    TASKFLOW_NAME       VARCHAR(200)  NOT NULL,   -- denormalised for easy querying
    JOB_NAME            VARCHAR(200)  NOT NULL,   -- must match ELT_JOB_CATALOG.JOB_NAME
    IS_CRITICAL         BOOLEAN DEFAULT TRUE,     -- FALSE = failure sets Taskflow to WARNING, not FAILED
    EXECUTION_ORDER     NUMBER DEFAULT 1,         -- parallel jobs share the same order number
    NOTES               VARCHAR(1000),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT uq_taskflow_job UNIQUE (TASKFLOW_ID, JOB_NAME)
)
COMMENT = 'Maps jobs to Taskflows and marks which jobs are critical vs non-critical.';


-- ── 1c. TASKFLOW RUN LOG ──────────────────────────────────────────────────
-- One row per Taskflow execution. Child job runs link back here via
-- TASKFLOW_RUN_ID on ELT_JOB_RUN_LOG.

CREATE TABLE IF NOT EXISTS ELT_TASKFLOW_RUN (
    TASKFLOW_RUN_ID     NUMBER AUTOINCREMENT PRIMARY KEY,
    TASKFLOW_ID         NUMBER        NOT NULL REFERENCES ELT_TASKFLOW_CATALOG(TASKFLOW_ID),
    TASKFLOW_NAME       VARCHAR(200)  NOT NULL,   -- denormalised for easy querying
    TOOL_NAME           VARCHAR(100),             -- 'IICS', 'MATILLION', etc.
    TOOL_TASKFLOW_ID    VARCHAR(500),             -- the ID of this run inside your ELT tool
    ENVIRONMENT         VARCHAR(50)   DEFAULT 'PROD',
    TRIGGER_TYPE        VARCHAR(20)   DEFAULT 'SCHEDULED',
                        -- SCHEDULED  = triggered automatically by a scheduler
                        -- MANUAL     = triggered by a person (ad-hoc rerun)
                        -- EVENT      = triggered by an upstream event or dependency
                        -- Enforced in sp_start_taskflow_run()

    -- Timing
    START_TIME          TIMESTAMP_NTZ NOT NULL,
    END_TIME            TIMESTAMP_NTZ,
    DURATION_SECONDS    NUMBER AS (
                            DATEDIFF('second', START_TIME,
                                COALESCE(END_TIME, CURRENT_TIMESTAMP()))
                        ),

    -- Outcome — derived from child job outcomes by sp_close_taskflow_run()
    -- RUNNING   = Taskflow is in progress
    -- SUCCESS   = all jobs completed successfully
    -- WARNING   = one or more non-critical jobs failed, all critical jobs succeeded
    -- FAILED    = one or more critical jobs failed
    -- PARTIAL   = Taskflow was stopped before all jobs ran (e.g. early exit on critical failure)
    STATUS              VARCHAR(20)   DEFAULT 'RUNNING',

    -- Counts rolled up from child job runs
    TOTAL_JOBS          NUMBER DEFAULT 0,
    JOBS_SUCCEEDED      NUMBER DEFAULT 0,
    JOBS_FAILED         NUMBER DEFAULT 0,
    JOBS_WARNING        NUMBER DEFAULT 0,
    TOTAL_ROWS_INSERTED NUMBER DEFAULT 0,
    TOTAL_ROWS_REJECTED NUMBER DEFAULT 0,

    -- Trigger metadata
    TRIGGERED_BY        VARCHAR(200),
    TRIGGER_NOTES       VARCHAR(1000),   -- e.g. 'Manual rerun after source outage'

    -- Error summary (populated if Taskflow fails before any jobs start)
    ERROR_MESSAGE       VARCHAR(4000),

    NOTES               VARCHAR(2000),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'One row per Taskflow execution. Parent record for all child job runs.'
CLUSTER BY (TASKFLOW_ID, START_TIME);


-- ── 1d. ADD TASKFLOW_RUN_ID TO ELT_JOB_RUN_LOG ───────────────────────────
-- Links each individual job run back to its parent Taskflow run.
-- Nullable — standalone jobs that are not part of a Taskflow leave this NULL.

ALTER TABLE ELT_JOB_RUN_LOG
    ADD COLUMN IF NOT EXISTS TASKFLOW_RUN_ID  NUMBER       REFERENCES ELT_TASKFLOW_RUN(TASKFLOW_RUN_ID),
    ADD COLUMN IF NOT EXISTS IS_CRITICAL      BOOLEAN      DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS EXECUTION_ORDER  NUMBER       DEFAULT 1;

-- Note: existing rows will have TASKFLOW_RUN_ID = NULL, which is correct.
-- They are standalone job runs and are unaffected by this extension.


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 2 — STORED PROCEDURES
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 2a. REGISTER A TASKFLOW ───────────────────────────────────────────────
-- Call once when setting up a new Taskflow. Idempotent.

CREATE OR REPLACE PROCEDURE sp_register_taskflow(
    p_taskflow_name     VARCHAR,
    p_description       VARCHAR,
    p_schedule          VARCHAR,
    p_owner_team        VARCHAR,
    p_tool_name         VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO ELT_TASKFLOW_CATALOG AS target
    USING (
        SELECT
            :p_taskflow_name  AS TASKFLOW_NAME,
            :p_description    AS TASKFLOW_DESC,
            :p_schedule       AS SCHEDULE,
            :p_owner_team     AS OWNER_TEAM,
            :p_tool_name      AS TOOL_NAME
    ) AS source ON target.TASKFLOW_NAME = source.TASKFLOW_NAME
    WHEN MATCHED THEN UPDATE SET
        TASKFLOW_DESC = source.TASKFLOW_DESC,
        SCHEDULE      = source.SCHEDULE,
        OWNER_TEAM    = source.OWNER_TEAM,
        TOOL_NAME     = source.TOOL_NAME,
        UPDATED_AT    = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        TASKFLOW_NAME, TASKFLOW_DESC, SCHEDULE, OWNER_TEAM, TOOL_NAME
    ) VALUES (
        source.TASKFLOW_NAME, source.TASKFLOW_DESC, source.SCHEDULE,
        source.OWNER_TEAM, source.TOOL_NAME
    );

    RETURN 'Taskflow registered: ' || :p_taskflow_name;
END;
$$
COMMENT = 'Register or update a Taskflow in the catalog. Safe to call multiple times.';


-- ── 2b. REGISTER A JOB WITHIN A TASKFLOW ─────────────────────────────────
-- Call once per job when defining the Taskflow membership.
-- IS_CRITICAL = TRUE  → failure causes overall Taskflow status = FAILED
-- IS_CRITICAL = FALSE → failure causes overall Taskflow status = WARNING only
-- EXECUTION_ORDER: parallel jobs share the same value (e.g. 2,2,2 = run together)

CREATE OR REPLACE PROCEDURE sp_register_taskflow_job(
    p_taskflow_name     VARCHAR,
    p_job_name          VARCHAR,
    p_is_critical       BOOLEAN DEFAULT TRUE,
    p_execution_order   NUMBER  DEFAULT 1,
    p_notes             VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_taskflow_id NUMBER;
BEGIN
    SELECT TASKFLOW_ID INTO v_taskflow_id
    FROM ELT_TASKFLOW_CATALOG
    WHERE TASKFLOW_NAME = :p_taskflow_name AND IS_ACTIVE = TRUE;

    IF (v_taskflow_id IS NULL) THEN
        RAISE EXCEPTION 'Taskflow not found: %. Run sp_register_taskflow first.', :p_taskflow_name;
    END IF;

    MERGE INTO ELT_TASKFLOW_JOBS AS target
    USING (
        SELECT
            v_taskflow_id     AS TASKFLOW_ID,
            :p_taskflow_name  AS TASKFLOW_NAME,
            :p_job_name       AS JOB_NAME,
            :p_is_critical    AS IS_CRITICAL,
            :p_execution_order AS EXECUTION_ORDER,
            :p_notes          AS NOTES
    ) AS source
        ON target.TASKFLOW_ID = source.TASKFLOW_ID
       AND target.JOB_NAME    = source.JOB_NAME
    WHEN MATCHED THEN UPDATE SET
        IS_CRITICAL     = source.IS_CRITICAL,
        EXECUTION_ORDER = source.EXECUTION_ORDER,
        NOTES           = source.NOTES
    WHEN NOT MATCHED THEN INSERT (
        TASKFLOW_ID, TASKFLOW_NAME, JOB_NAME,
        IS_CRITICAL, EXECUTION_ORDER, NOTES
    ) VALUES (
        source.TASKFLOW_ID, source.TASKFLOW_NAME, source.JOB_NAME,
        source.IS_CRITICAL, source.EXECUTION_ORDER, source.NOTES
    );

    RETURN :p_job_name || ' registered in Taskflow ' || :p_taskflow_name
        || ' (critical: ' || :p_is_critical || ', order: ' || :p_execution_order || ')';
END;
$$
COMMENT = 'Add a job to a Taskflow definition. IS_CRITICAL controls failure escalation.';


-- ── 2c. START A TASKFLOW RUN ──────────────────────────────────────────────
-- Call at the very beginning of the Taskflow, before any jobs start.
-- Returns TASKFLOW_RUN_ID — store this and pass it to every sp_start_job_run
-- call within this Taskflow execution.

CREATE OR REPLACE PROCEDURE sp_start_taskflow_run(
    p_taskflow_name     VARCHAR,
    p_tool_name         VARCHAR,
    p_tool_taskflow_id  VARCHAR,
    p_environment       VARCHAR   DEFAULT 'PROD',
    p_trigger_type      VARCHAR   DEFAULT 'SCHEDULED',
    p_triggered_by      VARCHAR   DEFAULT NULL,
    p_trigger_notes     VARCHAR   DEFAULT NULL
)
RETURNS NUMBER  -- returns TASKFLOW_RUN_ID
LANGUAGE SQL
AS
$$
DECLARE
    v_taskflow_id     NUMBER;
    v_taskflow_run_id NUMBER;
    v_total_jobs      NUMBER;
BEGIN
    -- Validate trigger type
    IF (UPPER(:p_trigger_type) NOT IN ('SCHEDULED', 'MANUAL', 'EVENT')) THEN
        RAISE EXCEPTION 'Invalid trigger type "%". Must be SCHEDULED, MANUAL, or EVENT.', :p_trigger_type;
    END IF;

    SELECT TASKFLOW_ID INTO v_taskflow_id
    FROM ELT_TASKFLOW_CATALOG
    WHERE TASKFLOW_NAME = :p_taskflow_name AND IS_ACTIVE = TRUE;

    IF (v_taskflow_id IS NULL) THEN
        RAISE EXCEPTION 'Taskflow not found: %. Run sp_register_taskflow first.', :p_taskflow_name;
    END IF;

    -- Count how many jobs are registered for this Taskflow (for the summary)
    SELECT COUNT(*) INTO v_total_jobs
    FROM ELT_TASKFLOW_JOBS
    WHERE TASKFLOW_ID = v_taskflow_id;

    INSERT INTO ELT_TASKFLOW_RUN (
        TASKFLOW_ID, TASKFLOW_NAME, TOOL_NAME, TOOL_TASKFLOW_ID,
        ENVIRONMENT, TRIGGER_TYPE, TRIGGERED_BY, TRIGGER_NOTES,
        START_TIME, STATUS, TOTAL_JOBS
    )
    VALUES (
        v_taskflow_id, :p_taskflow_name, UPPER(:p_tool_name), :p_tool_taskflow_id,
        UPPER(:p_environment), UPPER(:p_trigger_type), :p_triggered_by, :p_trigger_notes,
        CURRENT_TIMESTAMP(), 'RUNNING', v_total_jobs
    );

    SELECT MAX(TASKFLOW_RUN_ID) INTO v_taskflow_run_id
    FROM ELT_TASKFLOW_RUN
    WHERE TASKFLOW_NAME  = :p_taskflow_name
      AND STATUS         = 'RUNNING'
      AND TRIGGERED_BY   = :p_triggered_by;

    RETURN v_taskflow_run_id;
END;
$$
COMMENT = 'Opens a Taskflow run record. Returns TASKFLOW_RUN_ID — pass this to every sp_start_job_run call within this Taskflow.';


-- ── 2d. MODIFIED sp_start_job_run ─────────────────────────────────────────
-- Replaces the original sp_start_job_run to accept an optional
-- TASKFLOW_RUN_ID. Fully backwards compatible — existing calls without
-- TASKFLOW_RUN_ID continue to work exactly as before.

CREATE OR REPLACE PROCEDURE sp_start_job_run(
    p_job_name              VARCHAR,
    p_tool_name             VARCHAR,
    p_tool_job_id           VARCHAR,
    p_environment           VARCHAR,
    p_triggered_by          VARCHAR,
    p_load_window_start     TIMESTAMP_NTZ DEFAULT NULL,
    p_load_window_end       TIMESTAMP_NTZ DEFAULT NULL,
    p_taskflow_run_id       NUMBER        DEFAULT NULL,  -- NEW: link to parent Taskflow
    p_is_critical           BOOLEAN       DEFAULT TRUE,  -- NEW: from ELT_TASKFLOW_JOBS
    p_execution_order       NUMBER        DEFAULT 1      -- NEW: position within Taskflow
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
DECLARE
    v_job_id NUMBER;
    v_run_id NUMBER;
    v_is_critical BOOLEAN;
    v_execution_order NUMBER;
BEGIN
    SELECT JOB_ID INTO v_job_id
    FROM ELT_JOB_CATALOG
    WHERE JOB_NAME = :p_job_name AND IS_ACTIVE = TRUE;

    IF (v_job_id IS NULL) THEN
        RAISE EXCEPTION 'Job not found in catalog: %. Run sp_register_job first.', :p_job_name;
    END IF;

    -- If a TASKFLOW_RUN_ID was supplied, look up the IS_CRITICAL and
    -- EXECUTION_ORDER from the Taskflow job definition (overrides passed params
    -- if the job is registered in the Taskflow catalog)
    IF (:p_taskflow_run_id IS NOT NULL) THEN
        SELECT tj.IS_CRITICAL, tj.EXECUTION_ORDER
        INTO   v_is_critical, v_execution_order
        FROM   ELT_TASKFLOW_RUN  tr
        JOIN   ELT_TASKFLOW_JOBS tj
            ON tr.TASKFLOW_ID = tj.TASKFLOW_ID
           AND tj.JOB_NAME    = :p_job_name
        WHERE  tr.TASKFLOW_RUN_ID = :p_taskflow_run_id;
    END IF;

    -- Fall back to passed params if not found in catalog
    v_is_critical     := COALESCE(v_is_critical,     :p_is_critical);
    v_execution_order := COALESCE(v_execution_order, :p_execution_order);

    INSERT INTO ELT_JOB_RUN_LOG (
        JOB_ID, JOB_NAME, TOOL_NAME, TOOL_JOB_ID,
        ENVIRONMENT, START_TIME, STATUS,
        TRIGGERED_BY, LOAD_WINDOW_START, LOAD_WINDOW_END,
        TASKFLOW_RUN_ID, IS_CRITICAL, EXECUTION_ORDER
    )
    VALUES (
        v_job_id, :p_job_name, UPPER(:p_tool_name), :p_tool_job_id,
        UPPER(:p_environment), CURRENT_TIMESTAMP(), 'RUNNING',
        :p_triggered_by, :p_load_window_start, :p_load_window_end,
        :p_taskflow_run_id, v_is_critical, v_execution_order
    );

    SELECT MAX(RUN_ID) INTO v_run_id
    FROM ELT_JOB_RUN_LOG
    WHERE JOB_NAME   = :p_job_name
      AND STATUS     = 'RUNNING'
      AND TRIGGERED_BY = :p_triggered_by;

    RETURN v_run_id;
END;
$$
COMMENT = 'Opens a job run record. Pass TASKFLOW_RUN_ID to link this job to a parent Taskflow. Backwards compatible — omit TASKFLOW_RUN_ID for standalone jobs.';


-- ── 2e. CLOSE A TASKFLOW RUN ──────────────────────────────────────────────
-- Call at the very end of the Taskflow after all jobs have finished.
-- Automatically derives the overall Taskflow status from the child job outcomes:
--   All critical jobs succeeded                    → SUCCESS
--   All critical jobs succeeded, some warnings     → WARNING
--   Any critical job failed                        → FAILED
--   Taskflow stopped before all jobs ran           → PARTIAL

CREATE OR REPLACE PROCEDURE sp_close_taskflow_run(
    p_taskflow_run_id   NUMBER,
    p_notes             VARCHAR DEFAULT NULL,
    p_error_message     VARCHAR DEFAULT NULL   -- populate if Taskflow failed before any jobs ran
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_jobs_succeeded      NUMBER DEFAULT 0;
    v_jobs_failed_critical NUMBER DEFAULT 0;
    v_jobs_failed_noncrit  NUMBER DEFAULT 0;
    v_jobs_warning        NUMBER DEFAULT 0;
    v_jobs_ran            NUMBER DEFAULT 0;
    v_total_jobs          NUMBER DEFAULT 0;
    v_total_rows_inserted NUMBER DEFAULT 0;
    v_total_rows_rejected NUMBER DEFAULT 0;
    v_derived_status      VARCHAR;
BEGIN
    -- Aggregate child job run outcomes
    SELECT
        COUNT(*)                                                          AS jobs_ran,
        SUM(CASE WHEN STATUS = 'SUCCESS'                THEN 1 ELSE 0 END) AS jobs_succeeded,
        SUM(CASE WHEN STATUS = 'FAILED' AND IS_CRITICAL  THEN 1 ELSE 0 END) AS jobs_failed_critical,
        SUM(CASE WHEN STATUS = 'FAILED' AND NOT IS_CRITICAL THEN 1 ELSE 0 END) AS jobs_failed_noncrit,
        SUM(CASE WHEN STATUS = 'WARNING'               THEN 1 ELSE 0 END) AS jobs_warning,
        SUM(COALESCE(ROWS_INSERTED, 0))                                   AS total_rows_inserted,
        SUM(COALESCE(ROWS_REJECTED, 0))                                   AS total_rows_rejected
    INTO
        v_jobs_ran, v_jobs_succeeded, v_jobs_failed_critical,
        v_jobs_failed_noncrit, v_jobs_warning,
        v_total_rows_inserted, v_total_rows_rejected
    FROM ELT_JOB_RUN_LOG
    WHERE TASKFLOW_RUN_ID = :p_taskflow_run_id;

    SELECT TOTAL_JOBS INTO v_total_jobs
    FROM ELT_TASKFLOW_RUN
    WHERE TASKFLOW_RUN_ID = :p_taskflow_run_id;

    -- Derive overall Taskflow status
    v_derived_status :=
        CASE
            -- Error message supplied means Taskflow failed before jobs ran
            WHEN :p_error_message IS NOT NULL
                THEN 'FAILED'
            -- Any critical job failed
            WHEN v_jobs_failed_critical > 0
                THEN 'FAILED'
            -- Fewer jobs ran than were registered (early exit)
            WHEN v_jobs_ran < v_total_jobs AND v_total_jobs > 0
                THEN 'PARTIAL'
            -- Non-critical failures or warnings present
            WHEN v_jobs_failed_noncrit > 0 OR v_jobs_warning > 0
                THEN 'WARNING'
            -- Everything succeeded
            ELSE 'SUCCESS'
        END;

    UPDATE ELT_TASKFLOW_RUN
    SET
        END_TIME            = CURRENT_TIMESTAMP(),
        STATUS              = v_derived_status,
        JOBS_SUCCEEDED      = v_jobs_succeeded,
        JOBS_FAILED         = v_jobs_failed_critical + v_jobs_failed_noncrit,
        JOBS_WARNING        = v_jobs_warning,
        TOTAL_ROWS_INSERTED = v_total_rows_inserted,
        TOTAL_ROWS_REJECTED = v_total_rows_rejected,
        ERROR_MESSAGE       = :p_error_message,
        NOTES               = :p_notes
    WHERE TASKFLOW_RUN_ID = :p_taskflow_run_id;

    RETURN 'Taskflow run ' || :p_taskflow_run_id
        || ' closed with status: ' || v_derived_status
        || ' (' || v_jobs_succeeded || ' succeeded, '
        || (v_jobs_failed_critical + v_jobs_failed_noncrit) || ' failed)';
END;
$$
COMMENT = 'Closes a Taskflow run. Automatically derives overall status from child job outcomes.';


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 3 — REPORTING VIEWS
   ───────────────────────────────────────────────────────────────────────────*/

-- ── 3a. TASKFLOW SUMMARY ──────────────────────────────────────────────────
-- One row per registered Taskflow with last run info and success rate.
-- Your top-level daily health check for orchestrations.

CREATE OR REPLACE VIEW VW_TASKFLOW_SUMMARY AS
SELECT
    c.TASKFLOW_NAME,
    c.SCHEDULE,
    c.OWNER_TEAM,
    c.TOOL_NAME,
    COUNT(r.TASKFLOW_RUN_ID)                                             AS TOTAL_RUNS,
    SUM(CASE WHEN r.STATUS = 'SUCCESS' THEN 1 ELSE 0 END)               AS SUCCESSFUL_RUNS,
    SUM(CASE WHEN r.STATUS = 'FAILED'  THEN 1 ELSE 0 END)               AS FAILED_RUNS,
    SUM(CASE WHEN r.STATUS = 'WARNING' THEN 1 ELSE 0 END)               AS WARNING_RUNS,
    SUM(CASE WHEN r.STATUS = 'PARTIAL' THEN 1 ELSE 0 END)               AS PARTIAL_RUNS,
    ROUND(
        SUM(CASE WHEN r.STATUS = 'SUCCESS' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(r.TASKFLOW_RUN_ID), 0) * 100, 1
    )                                                                    AS SUCCESS_RATE_PCT,
    MAX(r.START_TIME)                                                    AS LAST_RUN_START,
    MAX(CASE WHEN r.STATUS = 'SUCCESS' THEN r.END_TIME END)             AS LAST_SUCCESSFUL_RUN,
    AVG(r.DURATION_SECONDS)                                             AS AVG_DURATION_SECONDS,
    SUM(r.TOTAL_ROWS_INSERTED)                                          AS ALL_TIME_ROWS_INSERTED
FROM ELT_TASKFLOW_CATALOG c
LEFT JOIN ELT_TASKFLOW_RUN r ON c.TASKFLOW_ID = r.TASKFLOW_ID
WHERE c.IS_ACTIVE = TRUE
GROUP BY 1,2,3,4
ORDER BY c.TASKFLOW_NAME;


-- ── 3b. TASKFLOW RUN HISTORY ──────────────────────────────────────────────
-- Every Taskflow execution with its child job summary counts.

CREATE OR REPLACE VIEW VW_TASKFLOW_RUN_HISTORY AS
SELECT
    r.TASKFLOW_RUN_ID,
    r.TASKFLOW_NAME,
    r.TOOL_NAME,
    r.ENVIRONMENT,
    r.TRIGGER_TYPE,
    r.TRIGGERED_BY,
    r.STATUS,
    r.START_TIME,
    r.END_TIME,
    r.DURATION_SECONDS,
    r.TOTAL_JOBS,
    r.JOBS_SUCCEEDED,
    r.JOBS_FAILED,
    r.JOBS_WARNING,
    r.TOTAL_ROWS_INSERTED,
    r.TOTAL_ROWS_REJECTED,
    r.TRIGGER_NOTES,
    r.ERROR_MESSAGE,
    r.NOTES
FROM ELT_TASKFLOW_RUN r
ORDER BY r.START_TIME DESC;


-- ── 3c. TASKFLOW DRILL-DOWN ───────────────────────────────────────────────
-- For a given Taskflow run, shows every child job with its individual outcome.
-- Use this when a Taskflow shows FAILED or WARNING and you need to know which
-- specific job caused it.

CREATE OR REPLACE VIEW VW_TASKFLOW_JOB_DETAIL AS
SELECT
    r.TASKFLOW_RUN_ID,
    r.TASKFLOW_NAME,
    r.STATUS                                        AS TASKFLOW_STATUS,
    r.START_TIME                                    AS TASKFLOW_START,
    j.RUN_ID,
    j.JOB_NAME,
    j.IS_CRITICAL,
    j.EXECUTION_ORDER,
    j.STATUS                                        AS JOB_STATUS,
    j.START_TIME                                    AS JOB_START,
    j.END_TIME                                      AS JOB_END,
    j.DURATION_SECONDS                              AS JOB_DURATION_SECONDS,
    j.ROWS_INSERTED,
    j.ROWS_REJECTED,
    j.ERROR_MESSAGE                                 AS JOB_ERROR_MESSAGE,
    j.TOOL_JOB_ID
FROM ELT_TASKFLOW_RUN r
JOIN ELT_JOB_RUN_LOG  j ON r.TASKFLOW_RUN_ID = j.TASKFLOW_RUN_ID
ORDER BY r.START_TIME DESC, j.EXECUTION_ORDER ASC, j.START_TIME ASC;


-- ── 3d. FAILED AND WARNING TASKFLOWS ─────────────────────────────────────
-- Every Taskflow run that did not fully succeed, with the names of the
-- jobs that caused the issue.

CREATE OR REPLACE VIEW VW_TASKFLOW_FAILURES AS
SELECT
    r.TASKFLOW_RUN_ID,
    r.TASKFLOW_NAME,
    r.STATUS                                        AS TASKFLOW_STATUS,
    r.START_TIME,
    r.END_TIME,
    r.DURATION_SECONDS,
    r.TRIGGER_TYPE,
    r.TRIGGERED_BY,
    r.JOBS_FAILED,
    r.ERROR_MESSAGE                                 AS TASKFLOW_ERROR,
    -- Comma-separated list of failed jobs for quick diagnosis
    LISTAGG(
        CASE WHEN j.STATUS = 'FAILED'
             THEN j.JOB_NAME || CASE WHEN j.IS_CRITICAL THEN ' [CRITICAL]' ELSE ' [non-critical]' END
        END, ', '
    ) WITHIN GROUP (ORDER BY j.JOB_NAME)           AS FAILED_JOBS,
    LISTAGG(
        CASE WHEN j.STATUS = 'FAILED' THEN j.ERROR_MESSAGE END, ' | '
    ) WITHIN GROUP (ORDER BY j.JOB_NAME)           AS FAILED_JOB_ERRORS
FROM ELT_TASKFLOW_RUN r
LEFT JOIN ELT_JOB_RUN_LOG j ON r.TASKFLOW_RUN_ID = j.TASKFLOW_RUN_ID
WHERE r.STATUS IN ('FAILED', 'WARNING', 'PARTIAL')
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY r.START_TIME DESC;


-- ── 3e. TASKFLOW STRUCTURE REFERENCE ─────────────────────────────────────
-- Shows the catalog-level definition of each Taskflow — which jobs belong
-- to it, their criticality, and their execution order. Useful for onboarding
-- and documentation.

CREATE OR REPLACE VIEW VW_TASKFLOW_STRUCTURE AS
SELECT
    c.TASKFLOW_NAME,
    c.TASKFLOW_DESC,
    c.SCHEDULE,
    c.OWNER_TEAM,
    tj.EXECUTION_ORDER,
    tj.JOB_NAME,
    tj.IS_CRITICAL,
    tj.NOTES                                        AS JOB_NOTES,
    jc.SOURCE_SYSTEM,
    jc.TARGET_SCHEMA || '.' || jc.TARGET_TABLE      AS TARGET_TABLE,
    jc.JOB_CATEGORY
FROM ELT_TASKFLOW_CATALOG c
JOIN ELT_TASKFLOW_JOBS    tj ON c.TASKFLOW_ID = tj.TASKFLOW_ID
LEFT JOIN ELT_JOB_CATALOG jc ON tj.JOB_NAME   = jc.JOB_NAME
WHERE c.IS_ACTIVE = TRUE
ORDER BY c.TASKFLOW_NAME, tj.EXECUTION_ORDER, tj.JOB_NAME;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 4 — PERMISSIONS
   Grant the ELT service account access to the new objects.
   Run as SECURITYADMIN. Replace ELT_ROLE with your actual role name.
   ───────────────────────────────────────────────────────────────────────────*/

USE ROLE SECURITYADMIN;

GRANT SELECT ON TABLE ANALYTICS_DB.ELT_AUDIT.ELT_TASKFLOW_CATALOG TO ROLE ELT_ROLE;
GRANT SELECT ON TABLE ANALYTICS_DB.ELT_AUDIT.ELT_TASKFLOW_RUN     TO ROLE ELT_ROLE;
GRANT SELECT ON TABLE ANALYTICS_DB.ELT_AUDIT.ELT_TASKFLOW_JOBS    TO ROLE ELT_ROLE;

GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_SUMMARY      TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_RUN_HISTORY  TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_JOB_DETAIL   TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_FAILURES      TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_STRUCTURE     TO ROLE ELT_ROLE;

GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_SUMMARY      TO ROLE ELT_AUDIT_READER;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_RUN_HISTORY  TO ROLE ELT_AUDIT_READER;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_JOB_DETAIL   TO ROLE ELT_AUDIT_READER;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_FAILURES      TO ROLE ELT_AUDIT_READER;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_TASKFLOW_STRUCTURE     TO ROLE ELT_AUDIT_READER;

USE ROLE SECURITYADMIN;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_register_taskflow(
    VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_register_taskflow_job(
    VARCHAR, VARCHAR, BOOLEAN, NUMBER, VARCHAR
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_start_taskflow_run(
    VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_start_job_run(
    VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, TIMESTAMP_NTZ, TIMESTAMP_NTZ, NUMBER, BOOLEAN, NUMBER
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_close_taskflow_run(
    NUMBER, VARCHAR, VARCHAR
) TO ROLE ELT_ROLE;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 5 — EXAMPLE: DAILY_SALES_LOAD TASKFLOW
   A realistic example with sequential and parallel jobs, one non-critical.
   Copy and adapt for your own Taskflows.
   ───────────────────────────────────────────────────────────────────────────*/

-- Step 1: Register the Taskflow
CALL sp_register_taskflow(
    'DAILY_SALES_LOAD',
    'Loads all daily sales data: customers, products, and transactions',
    'Daily 02:00 UTC',
    'Data Engineering',
    'IICS'
);

-- Step 2: Register the jobs that belong to it
-- Order 1 — runs first, must succeed before anything else starts
CALL sp_register_taskflow_job('DAILY_SALES_LOAD', 'LOAD_CUSTOMER_DIM_DAILY',   TRUE,  1, 'Must complete before fact load');
CALL sp_register_taskflow_job('DAILY_SALES_LOAD', 'LOAD_PRODUCT_DIM_DAILY',    TRUE,  1, 'Must complete before fact load');

-- Order 2 — runs after dims, both in parallel, both critical
CALL sp_register_taskflow_job('DAILY_SALES_LOAD', 'LOAD_SALES_FACT_DAILY',     TRUE,  2, NULL);
CALL sp_register_taskflow_job('DAILY_SALES_LOAD', 'LOAD_RETURNS_FACT_DAILY',   TRUE,  2, NULL);

-- Order 3 — runs last, non-critical (reporting summary — nice to have but
-- failure does not block downstream consumers of the fact tables)
CALL sp_register_taskflow_job('DAILY_SALES_LOAD', 'REFRESH_SALES_SUMMARY',     FALSE, 3, 'Non-critical: failure raises WARNING only');


-- Step 3: At Taskflow runtime — open the Taskflow record
-- (This is what your IICS Taskflow calls at the very start)
CALL sp_start_taskflow_run(
    'DAILY_SALES_LOAD',
    'IICS',
    'TF_DAILY_SALES_20240115_0200',   -- your IICS Taskflow run ID
    'PROD',
    'SCHEDULED',
    'svc_informatica',
    NULL
);
-- Store the returned TASKFLOW_RUN_ID (assume it is 1)

-- Step 4: For each job, call sp_start_job_run with the TASKFLOW_RUN_ID
-- (IS_CRITICAL and EXECUTION_ORDER are looked up automatically from the catalog)
CALL sp_start_job_run(
    'LOAD_CUSTOMER_DIM_DAILY',
    'IICS',
    'MT_LOAD_CUSTOMER_20240115',
    'PROD',
    'svc_informatica',
    NULL, NULL,
    1     -- TASKFLOW_RUN_ID
);
-- ... same for each other job in the Taskflow

-- Step 5: After all jobs finish, close the Taskflow
-- Status is derived automatically from child outcomes
CALL sp_close_taskflow_run(
    1,      -- TASKFLOW_RUN_ID
    'Normal scheduled run',
    NULL    -- no top-level error
);


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 6 — KEY MONITORING QUERIES
   ───────────────────────────────────────────────────────────────────────────*/

-- Morning health check — all Taskflows
SELECT TASKFLOW_NAME, LAST_RUN_START, STATUS, SUCCESS_RATE_PCT,
       TOTAL_ROWS_INSERTED, AVG_DURATION_SECONDS
FROM ELT_AUDIT.VW_TASKFLOW_SUMMARY
ORDER BY LAST_RUN_START DESC NULLS LAST;

-- What ran today at the Taskflow level?
SELECT TASKFLOW_RUN_ID, TASKFLOW_NAME, STATUS, START_TIME,
       DURATION_SECONDS, JOBS_SUCCEEDED, JOBS_FAILED, TOTAL_ROWS_INSERTED
FROM ELT_AUDIT.VW_TASKFLOW_RUN_HISTORY
WHERE START_TIME >= CURRENT_DATE()
ORDER BY START_TIME DESC;

-- Drill into a specific Taskflow run to see every child job
SELECT JOB_NAME, IS_CRITICAL, EXECUTION_ORDER, JOB_STATUS,
       JOB_DURATION_SECONDS, ROWS_INSERTED, ROWS_REJECTED, JOB_ERROR_MESSAGE
FROM ELT_AUDIT.VW_TASKFLOW_JOB_DETAIL
WHERE TASKFLOW_RUN_ID = 1   -- replace with your TASKFLOW_RUN_ID
ORDER BY EXECUTION_ORDER, JOB_START;

-- Which Taskflows failed or warned recently?
SELECT TASKFLOW_RUN_ID, TASKFLOW_NAME, TASKFLOW_STATUS,
       START_TIME, FAILED_JOBS, FAILED_JOB_ERRORS
FROM ELT_AUDIT.VW_TASKFLOW_FAILURES
WHERE START_TIME >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY START_TIME DESC;

-- View the full structure of a Taskflow (which jobs, what order, criticality)
SELECT EXECUTION_ORDER, JOB_NAME, IS_CRITICAL, TARGET_TABLE, JOB_CATEGORY, JOB_NOTES
FROM ELT_AUDIT.VW_TASKFLOW_STRUCTURE
WHERE TASKFLOW_NAME = 'DAILY_SALES_LOAD'
ORDER BY EXECUTION_ORDER, JOB_NAME;


/* ════════════════════════════════════════════════════════════════════════════
   MODULE 4 — PERMISSIONS
   ════════════════════════════════════════════════════════════════════════════*/
/*
  PURPOSE: Creates two roles (ELT_ROLE for service accounts, ELT_AUDIT_READER
  for analysts/BI tools) and grants appropriate access to all objects.
  Run as SECURITYADMIN.
*/

USE ROLE SECURITYADMIN;
USE ROLE SECURITYADMIN;


/* ─────────────────────────────────────────────────────────────────────────────
   STEP 1 — CREATE A DEDICATED ROLE  (recommended approach)
   Rather than granting permissions directly to the service account, create
   a role. This makes it easy to grant the same permissions to future accounts,
   developers who need read access, or a second tool later (e.g. Matillion).
   ───────────────────────────────────────────────────────────────────────────*/

CREATE ROLE IF NOT EXISTS ELT_ROLE
  COMMENT = 'Role for ELT pipelines to write to the audit framework';

-- Assign the role to your service account
GRANT ROLE ELT_ROLE TO USER SVC_INFORMATICA;

-- Also assign to a human developer account so you can test manually
-- GRANT ROLE ELT_ROLE TO USER your_dev_username;


/* ─────────────────────────────────────────────────────────────────────────────
   STEP 2 — WAREHOUSE ACCESS
   The service account needs a warehouse to execute queries.
   Grant USAGE on whichever warehouse your ELT tool uses.
   ───────────────────────────────────────────────────────────────────────────*/

GRANT USAGE ON WAREHOUSE ELT_WH TO ROLE ELT_ROLE;
-- Replace ELT_WH with your actual warehouse name


/* ─────────────────────────────────────────────────────────────────────────────
   STEP 3 — DATABASE AND SCHEMA ACCESS
   The service account needs to be able to see the database and schema.
   USAGE on both is required before any object-level grants will work.
   ───────────────────────────────────────────────────────────────────────────*/

GRANT USAGE ON DATABASE ANALYTICS_DB       TO ROLE ELT_ROLE;
GRANT USAGE ON SCHEMA   ANALYTICS_DB.ELT_AUDIT  TO ROLE ELT_ROLE;


/* ─────────────────────────────────────────────────────────────────────────────
   STEP 4 — STORED PROCEDURE EXECUTION
   The service account calls six stored procedures. It needs USAGE on each one.
   
   NOTE: In Snowflake, stored procedures run with the privileges of the OWNER
   by default (called "owner's rights"). This means the procedure itself can
   INSERT/UPDATE the audit tables even if the calling role cannot — which is
   exactly what we want. The service account only needs USAGE on the procedures,
   not direct INSERT/UPDATE on the tables.
   ───────────────────────────────────────────────────────────────────────────*/

-- Core framework procedures
GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_register_job(
    VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_start_job_run(
    VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, TIMESTAMP_NTZ, TIMESTAMP_NTZ
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_end_job_run(
    NUMBER, VARCHAR, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, VARCHAR, VARCHAR, VARCHAR
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_log_error(
    NUMBER, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARIANT
) TO ROLE ELT_ROLE;

-- File tracking extension procedures (only needed for file-based pipelines)
GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_start_file(
    NUMBER, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, NUMBER, VARCHAR, TIMESTAMP_NTZ, NUMBER, VARCHAR, VARCHAR
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_end_file(
    NUMBER, VARCHAR, NUMBER, NUMBER, NUMBER, NUMBER, VARCHAR, VARCHAR, VARCHAR
) TO ROLE ELT_ROLE;

GRANT USAGE ON PROCEDURE ANALYTICS_DB.ELT_AUDIT.sp_register_file_batch(
    NUMBER, VARCHAR, VARIANT, VARCHAR, VARCHAR, VARCHAR, VARCHAR
) TO ROLE ELT_ROLE;


/* ─────────────────────────────────────────────────────────────────────────────
   STEP 5 — VIEW ACCESS  (for the service account to query run history)
   This is optional — the ELT tool only needs to CALL procedures.
   Grant SELECT on views if you want the pipeline itself to query audit history,
   for example to determine the last successful load window for an incremental job.
   ───────────────────────────────────────────────────────────────────────────*/

GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_JOB_SUMMARY          TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_RECENT_RUN_HISTORY    TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_FAILED_RUNS           TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_ROW_COUNT_ANOMALIES   TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_FILE_HISTORY          TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_FILE_FAILURES         TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_UNPROCESSED_FILES     TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_DUPLICATE_FILES       TO ROLE ELT_ROLE;
GRANT SELECT ON VIEW ANALYTICS_DB.ELT_AUDIT.VW_FILE_PROCESSING_SUMMARY TO ROLE ELT_ROLE;


/* ─────────────────────────────────────────────────────────────────────────────
   STEP 6 — TABLE ACCESS  (only needed for the incremental load window query)
   The incremental load pattern queries ELT_JOB_RUN_LOG directly to find the
   last successful run's LOAD_WINDOW_END. Grant SELECT on this table if your
   pipelines use that pattern. Do NOT grant INSERT/UPDATE — all writes must go
   through the stored procedures.
   ───────────────────────────────────────────────────────────────────────────*/

GRANT SELECT ON TABLE ANALYTICS_DB.ELT_AUDIT.ELT_JOB_RUN_LOG  TO ROLE ELT_ROLE;
GRANT SELECT ON TABLE ANALYTICS_DB.ELT_AUDIT.ELT_FILE_LOG      TO ROLE ELT_ROLE;
-- Note: no INSERT, UPDATE, or DELETE — writes go through stored procedures only


/* ─────────────────────────────────────────────────────────────────────────────
   STEP 7 — VERIFY
   Run these as the service account to confirm everything is in place
   before connecting your ELT tool.
   ───────────────────────────────────────────────────────────────────────────*/

-- Switch to the service account role to test
USE ROLE ELT_ROLE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA ELT_AUDIT;

-- Should return the list of procedures the role can call
SHOW PROCEDURES IN SCHEMA ELT_AUDIT;

-- Should return the list of views the role can query
SHOW VIEWS IN SCHEMA ELT_AUDIT;

-- Quick smoke test — register a test job and confirm it appears
CALL sp_register_job(
    'TEST_PERMISSIONS_JOB',
    'Temporary job to verify service account permissions',
    'TEST',
    'TEST_SCHEMA',
    'TEST_TABLE',
    'FULL_LOAD',
    'Data Engineering'
);

-- Confirm it was created
SELECT JOB_NAME, IS_ACTIVE, CREATED_AT
FROM ELT_JOB_CATALOG
WHERE JOB_NAME = 'TEST_PERMISSIONS_JOB';

-- Clean up the test job
USE ROLE SECURITYADMIN;
UPDATE ANALYTICS_DB.ELT_AUDIT.ELT_JOB_CATALOG
SET IS_ACTIVE = FALSE
WHERE JOB_NAME = 'TEST_PERMISSIONS_JOB';


/* ─────────────────────────────────────────────────────────────────────────────
   SEPARATE READ-ONLY ROLE  (for analysts and monitoring dashboards)
   If you have analysts or a BI tool that needs to query the audit views
   for reporting, create a second read-only role rather than using ELT_ROLE.
   ───────────────────────────────────────────────────────────────────────────*/

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS ELT_AUDIT_READER
  COMMENT = 'Read-only access to ELT audit views for monitoring and reporting';

GRANT USAGE ON DATABASE   ANALYTICS_DB            TO ROLE ELT_AUDIT_READER;
GRANT USAGE ON SCHEMA     ANALYTICS_DB.ELT_AUDIT  TO ROLE ELT_AUDIT_READER;
GRANT USAGE ON WAREHOUSE  ELT_WH                  TO ROLE ELT_AUDIT_READER;

GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS_DB.ELT_AUDIT TO ROLE ELT_AUDIT_READER;

-- Grant to analysts or a BI service account
-- GRANT ROLE ELT_AUDIT_READER TO USER analyst_username;
-- GRANT ROLE ELT_AUDIT_READER TO USER svc_tableau;


/* ════════════════════════════════════════════════════════════════════════════
   MODULE 5 — SMOKE TEST
   Run these after deployment to verify everything is wired up correctly.
   ════════════════════════════════════════════════════════════════════════════*/

USE ROLE ELT_ROLE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA ELT_AUDIT;

-- 1. Register a Taskflow
CALL sp_register_taskflow(
    'SMOKE_TEST_FLOW',
    'Deployment smoke test Taskflow',
    'Ad-hoc',
    'Data Engineering',
    'IICS'
);

-- 2. Register two jobs
CALL sp_register_job('SMOKE_TEST_JOB_A', 'Smoke test job A', 'TEST', 'TEST', 'TABLE_A', 'FULL_LOAD', 'Data Engineering');
CALL sp_register_job('SMOKE_TEST_JOB_B', 'Smoke test job B', 'TEST', 'TEST', 'TABLE_B', 'FULL_LOAD', 'Data Engineering');

-- 3. Register jobs within the Taskflow
CALL sp_register_taskflow_job('SMOKE_TEST_FLOW', 'SMOKE_TEST_JOB_A', TRUE,  1, 'Critical job');
CALL sp_register_taskflow_job('SMOKE_TEST_FLOW', 'SMOKE_TEST_JOB_B', FALSE, 2, 'Non-critical job');

-- 4. Open a Taskflow run
CALL sp_start_taskflow_run(
    'SMOKE_TEST_FLOW', 'IICS', 'TF_SMOKE_001',
    'DEV', 'MANUAL', 'svc_informatica', 'Deployment smoke test'
);
-- Note the returned TASKFLOW_RUN_ID and substitute below

-- 5. Open job runs (substitute actual TASKFLOW_RUN_ID from step 4)
CALL sp_start_job_run('SMOKE_TEST_JOB_A', 'IICS', 'MT_SMOKE_A', 'DEV', 'svc_informatica', NULL, NULL, 1);
CALL sp_start_job_run('SMOKE_TEST_JOB_B', 'IICS', 'MT_SMOKE_B', 'DEV', 'svc_informatica', NULL, NULL, 1);
-- Note the returned RUN_IDs

-- 6. Close job runs (substitute actual RUN_IDs)
CALL sp_end_job_run(1, 'SUCCESS', 100, 100, 0, 0, 0, NULL, NULL, 'Smoke test');
CALL sp_end_job_run(2, 'SUCCESS', 50,  50,  0, 0, 0, NULL, NULL, 'Smoke test');

-- 7. Close the Taskflow
CALL sp_close_taskflow_run(1, 'Smoke test complete', NULL);

-- 8. Verify results in views
SELECT * FROM VW_TASKFLOW_SUMMARY    WHERE TASKFLOW_NAME = 'SMOKE_TEST_FLOW';
SELECT * FROM VW_TASKFLOW_JOB_DETAIL WHERE TASKFLOW_NAME = 'SMOKE_TEST_FLOW';
SELECT * FROM VW_JOB_SUMMARY         WHERE JOB_NAME LIKE 'SMOKE_TEST%';
SELECT * FROM VW_RECENT_RUN_HISTORY  WHERE JOB_NAME LIKE 'SMOKE_TEST%';

-- 9. Clean up smoke test records
USE ROLE SYSADMIN;
UPDATE ANALYTICS_DB.ELT_AUDIT.ELT_JOB_CATALOG
    SET IS_ACTIVE = FALSE WHERE JOB_NAME LIKE 'SMOKE_TEST%';
UPDATE ANALYTICS_DB.ELT_AUDIT.ELT_TASKFLOW_CATALOG
    SET IS_ACTIVE = FALSE WHERE TASKFLOW_NAME = 'SMOKE_TEST_FLOW';
