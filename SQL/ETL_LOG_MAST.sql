/* ============================================================
   SNOWFLAKE ELT AUDIT FRAMEWORK
   
   Captures orchestration-level and job-level ELT runtime results.
   Tool-agnostic: works with Informatica, Matillion, dbt, ADF, etc.
   
   DEPLOY ORDER:
     1. Schema
     2. Tables  (ELT_JOB_CATALOG, ELT_JOB_RUN_LOG, ELT_ERROR_LOG)
     3. Stored Procedures
     4. Views
   ============================================================ */


-- ============================================================
-- SECTION 1: SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS ELT_AUDIT
  COMMENT = 'Audit framework for all ELT pipeline run history, error logging, and job metadata.';


-- ============================================================
-- SECTION 2: TABLES
-- ============================================================

/* ------------------------------------------------------------
   2.1  ELT_JOB_CATALOG
   One row per registered pipeline job.
   Register a job here ONCE before it can be audited.
   ------------------------------------------------------------ */
CREATE TABLE IF NOT EXISTS ELT_AUDIT.ELT_JOB_CATALOG (
    -- Identity
    JOB_ID              NUMBER AUTOINCREMENT PRIMARY KEY,
    JOB_NAME            VARCHAR(200)    NOT NULL UNIQUE,   -- canonical name used across ALL tools

    -- Job metadata
    SOURCE_SYSTEM       VARCHAR(100)    NOT NULL,          -- e.g. 'SALESFORCE', 'ORACLE_ERP', 'S3_FILES'
    TARGET_SCHEMA       VARCHAR(100)    NOT NULL,          -- Snowflake schema receiving data
    TARGET_TABLE        VARCHAR(200)    NOT NULL,          -- Snowflake target table (fully qualified)
    LOAD_TYPE           VARCHAR(20)     NOT NULL           -- 'FULL_LOAD' or 'INCREMENTAL'
                        DEFAULT 'INCREMENTAL',
    OWNER_TEAM          VARCHAR(100),                      -- team responsible for this pipeline
    SCHEDULE            VARCHAR(100),                      -- cron or description, e.g. 'Daily 02:00 UTC'

    -- Lifecycle
    IS_ACTIVE           BOOLEAN         NOT NULL DEFAULT TRUE,
    CREATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT SYSDATE(),
    UPDATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT SYSDATE(),
    NOTES               VARCHAR(2000),

    CONSTRAINT chk_load_type CHECK (LOAD_TYPE IN ('FULL_LOAD', 'INCREMENTAL'))
);

COMMENT ON TABLE  ELT_AUDIT.ELT_JOB_CATALOG                IS 'Registry of all ELT pipeline jobs. Must be registered here before runs can be audited.';
COMMENT ON COLUMN ELT_AUDIT.ELT_JOB_CATALOG.JOB_NAME       IS 'Canonical job name. Must be identical in every tool that runs this pipeline.';
COMMENT ON COLUMN ELT_AUDIT.ELT_JOB_CATALOG.IS_ACTIVE       IS 'Set FALSE when a job is retired or migrated. Historical run data is preserved.';


/* ------------------------------------------------------------
   2.2  ELT_JOB_RUN_LOG  (ORCHESTRATION level)
   One row per pipeline execution. Captures runtime results,
   row counts, and load window for both full and incremental loads.
   ------------------------------------------------------------ */
CREATE TABLE IF NOT EXISTS ELT_AUDIT.ELT_JOB_RUN_LOG (
    -- Identity
    RUN_ID              NUMBER AUTOINCREMENT PRIMARY KEY,
    JOB_NAME            VARCHAR(200)    NOT NULL,          -- FK to ELT_JOB_CATALOG.JOB_NAME (soft)

    -- Tool identity (tool-agnostic design: just a VARCHAR)
    TOOL_NAME           VARCHAR(50)     NOT NULL,          -- 'INFORMATICA', 'MATILLION', 'DBT', 'ADF', etc.
    TOOL_JOB_ID         VARCHAR(500),                      -- tool's own run/workflow identifier
    ENVIRONMENT         VARCHAR(20)     NOT NULL           -- 'PROD', 'UAT', 'DEV'
                        DEFAULT 'PROD',
    TRIGGERED_BY        VARCHAR(200),                      -- service account or user name

    -- Timing
    START_TIME          TIMESTAMP_NTZ   NOT NULL DEFAULT SYSDATE(),
    END_TIME            TIMESTAMP_NTZ,
    DURATION_SECONDS    NUMBER,                            -- computed by sp_end_job_run

    -- Status
    STATUS              VARCHAR(20)     NOT NULL DEFAULT 'RUNNING',

    -- Row-level metrics
    ROWS_EXTRACTED      NUMBER          DEFAULT 0,
    ROWS_INSERTED       NUMBER          DEFAULT 0,
    ROWS_UPDATED        NUMBER          DEFAULT 0,
    ROWS_DELETED        NUMBER          DEFAULT 0,
    ROWS_REJECTED       NUMBER          DEFAULT 0,

    -- Incremental load window
    LOAD_WINDOW_START   TIMESTAMP_NTZ,                     -- start of data window processed
    LOAD_WINDOW_END     TIMESTAMP_NTZ,                     -- end of data window processed

    -- Failure detail
    ERROR_MESSAGE       VARCHAR(4000),
    ERROR_CODE          VARCHAR(200),

    NOTES               VARCHAR(2000),
    CREATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT SYSDATE(),

    CONSTRAINT chk_run_status   CHECK (STATUS   IN ('RUNNING', 'SUCCESS', 'FAILED', 'WARNING')),
    CONSTRAINT chk_environment  CHECK (ENVIRONMENT IN ('PROD', 'UAT', 'DEV'))
);

COMMENT ON TABLE  ELT_AUDIT.ELT_JOB_RUN_LOG                 IS 'One row per pipeline execution. Closed by sp_end_job_run on success or failure.';
COMMENT ON COLUMN ELT_AUDIT.ELT_JOB_RUN_LOG.TOOL_NAME       IS 'Identifies the ELT tool. Changing tools just changes this value - all history is preserved.';
COMMENT ON COLUMN ELT_AUDIT.ELT_JOB_RUN_LOG.STATUS          IS 'RUNNING = in progress | SUCCESS = completed cleanly | FAILED = errored | WARNING = completed with issues';
COMMENT ON COLUMN ELT_AUDIT.ELT_JOB_RUN_LOG.ROWS_REJECTED   IS 'Rows that failed validation. Monitor this — silent rejections are a common data quality issue.';
COMMENT ON COLUMN ELT_AUDIT.ELT_JOB_RUN_LOG.LOAD_WINDOW_END IS 'End of data window. For incremental jobs, the NEXT run uses this as its LOAD_WINDOW_START.';


/* ------------------------------------------------------------
   2.3  ELT_JOB_STEP_LOG  (JOB / TASK level)
   One row per individual step within an orchestration run.
   Allows granular tracking of which step within a workflow failed.
   ------------------------------------------------------------ */
CREATE TABLE IF NOT EXISTS ELT_AUDIT.ELT_JOB_STEP_LOG (
    -- Identity
    STEP_LOG_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID              NUMBER          NOT NULL,           -- FK to ELT_JOB_RUN_LOG.RUN_ID
    JOB_NAME            VARCHAR(200)    NOT NULL,

    -- Step identity
    STEP_NAME           VARCHAR(200)    NOT NULL,           -- e.g. 'EXTRACT_SALESFORCE', 'LOAD_FACT_SALES'
    STEP_TYPE           VARCHAR(50),                        -- 'EXTRACT', 'TRANSFORM', 'LOAD', 'VALIDATE', 'NOTIFY'
    STEP_ORDER          NUMBER,                             -- sequence within the orchestration

    -- Timing
    STEP_START_TIME     TIMESTAMP_NTZ   NOT NULL DEFAULT SYSDATE(),
    STEP_END_TIME       TIMESTAMP_NTZ,
    STEP_DURATION_SECS  NUMBER,

    -- Status & metrics
    STATUS              VARCHAR(20)     NOT NULL DEFAULT 'RUNNING',
    ROWS_PROCESSED      NUMBER          DEFAULT 0,
    ROWS_REJECTED       NUMBER          DEFAULT 0,

    -- Failure detail
    ERROR_MESSAGE       VARCHAR(4000),
    ERROR_CODE          VARCHAR(200),

    NOTES               VARCHAR(2000),
    CREATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT SYSDATE(),

    CONSTRAINT fk_step_run      FOREIGN KEY (RUN_ID) REFERENCES ELT_AUDIT.ELT_JOB_RUN_LOG(RUN_ID),
    CONSTRAINT chk_step_status  CHECK (STATUS IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED', 'WARNING')),
    CONSTRAINT chk_step_type    CHECK (STEP_TYPE IN ('EXTRACT', 'TRANSFORM', 'LOAD', 'VALIDATE', 'NOTIFY', 'OTHER') OR STEP_TYPE IS NULL)
);

COMMENT ON TABLE  ELT_AUDIT.ELT_JOB_STEP_LOG               IS 'Granular step-level audit within an orchestration run. One row per task/session/component.';
COMMENT ON COLUMN ELT_AUDIT.ELT_JOB_STEP_LOG.STEP_ORDER    IS 'Sequence number for ordering steps in a run. Useful for identifying exactly where a pipeline failed.';


/* ------------------------------------------------------------
   2.4  ELT_ERROR_LOG
   Detailed error records. Multiple errors can be logged per run.
   ------------------------------------------------------------ */
CREATE TABLE IF NOT EXISTS ELT_AUDIT.ELT_ERROR_LOG (
    ERROR_LOG_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID              NUMBER          NOT NULL,
    JOB_NAME            VARCHAR(200)    NOT NULL,
    STEP_LOG_ID         NUMBER,                            -- optional: link to a specific step

    -- Error detail
    ERROR_MESSAGE       VARCHAR(4000)   NOT NULL,
    ERROR_CONTEXT       VARCHAR(4000),                     -- stack trace, row data, or tool-specific context
    ERROR_CODE          VARCHAR(200),
    SEVERITY            VARCHAR(20)     NOT NULL DEFAULT 'ERROR',

    LOGGED_AT           TIMESTAMP_NTZ   NOT NULL DEFAULT SYSDATE(),

    CONSTRAINT fk_error_run     FOREIGN KEY (RUN_ID) REFERENCES ELT_AUDIT.ELT_JOB_RUN_LOG(RUN_ID),
    CONSTRAINT chk_severity     CHECK (SEVERITY IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL'))
);

COMMENT ON TABLE ELT_AUDIT.ELT_ERROR_LOG IS 'Detailed error log. Call sp_log_error() to populate. Multiple errors can be logged per run.';


-- ============================================================
-- SECTION 3: STORED PROCEDURES
-- ============================================================

/* ------------------------------------------------------------
   3.1  sp_register_job
   Registers a new pipeline in the catalog (idempotent - safe
   to call multiple times; will not create duplicates).
   ------------------------------------------------------------ */
CREATE OR REPLACE PROCEDURE ELT_AUDIT.sp_register_job(
    P_JOB_NAME      VARCHAR,
    P_SOURCE_SYSTEM VARCHAR,
    P_TARGET_SCHEMA VARCHAR,
    P_TARGET_TABLE  VARCHAR,
    P_LOAD_TYPE     VARCHAR,
    P_OWNER_TEAM    VARCHAR,
    P_SCHEDULE      VARCHAR,
    P_NOTES         VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_existing NUMBER;
BEGIN
    -- Check if already registered
    SELECT COUNT(*) INTO :v_existing
    FROM ELT_AUDIT.ELT_JOB_CATALOG
    WHERE JOB_NAME = :P_JOB_NAME;

    IF (v_existing = 0) THEN
        INSERT INTO ELT_AUDIT.ELT_JOB_CATALOG (
            JOB_NAME, SOURCE_SYSTEM, TARGET_SCHEMA, TARGET_TABLE,
            LOAD_TYPE, OWNER_TEAM, SCHEDULE, NOTES
        ) VALUES (
            :P_JOB_NAME, :P_SOURCE_SYSTEM, :P_TARGET_SCHEMA, :P_TARGET_TABLE,
            :P_LOAD_TYPE, :P_OWNER_TEAM, :P_SCHEDULE, :P_NOTES
        );
        RETURN 'REGISTERED: ' || :P_JOB_NAME;
    ELSE
        -- Update metadata in case details have changed
        UPDATE ELT_AUDIT.ELT_JOB_CATALOG
        SET    SOURCE_SYSTEM = :P_SOURCE_SYSTEM,
               TARGET_SCHEMA = :P_TARGET_SCHEMA,
               TARGET_TABLE  = :P_TARGET_TABLE,
               LOAD_TYPE     = :P_LOAD_TYPE,
               OWNER_TEAM    = :P_OWNER_TEAM,
               SCHEDULE      = :P_SCHEDULE,
               UPDATED_AT    = SYSDATE(),
               NOTES         = COALESCE(:P_NOTES, NOTES)
        WHERE  JOB_NAME = :P_JOB_NAME;
        RETURN 'ALREADY_REGISTERED (metadata updated): ' || :P_JOB_NAME;
    END IF;
END;
$$;

COMMENT ON PROCEDURE ELT_AUDIT.sp_register_job(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR)
    IS 'Idempotent job registration. Call once per pipeline before its first run. Safe to re-run - will update metadata if already registered.';


/* ------------------------------------------------------------
   3.2  sp_start_job_run
   Opens an orchestration-level run record.
   Returns the new RUN_ID - capture this and pass to sp_end_job_run.
   ------------------------------------------------------------ */
CREATE OR REPLACE PROCEDURE ELT_AUDIT.sp_start_job_run(
    P_JOB_NAME          VARCHAR,
    P_TOOL_NAME         VARCHAR,
    P_TOOL_JOB_ID       VARCHAR,
    P_ENVIRONMENT       VARCHAR,
    P_TRIGGERED_BY      VARCHAR,
    P_LOAD_WINDOW_START TIMESTAMP_NTZ   DEFAULT NULL
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
DECLARE
    v_new_run_id    NUMBER;
    v_job_exists    NUMBER;
BEGIN
    -- Guard: job must be registered
    SELECT COUNT(*) INTO :v_job_exists
    FROM ELT_AUDIT.ELT_JOB_CATALOG
    WHERE JOB_NAME = :P_JOB_NAME AND IS_ACTIVE = TRUE;

    IF (v_job_exists = 0) THEN
        RAISE EXCEPTION 'Job not found or inactive in ELT_JOB_CATALOG: %. Run sp_register_job() first.', :P_JOB_NAME;
    END IF;

    INSERT INTO ELT_AUDIT.ELT_JOB_RUN_LOG (
        JOB_NAME, TOOL_NAME, TOOL_JOB_ID, ENVIRONMENT,
        TRIGGERED_BY, STATUS, LOAD_WINDOW_START
    ) VALUES (
        :P_JOB_NAME, :P_TOOL_NAME, :P_TOOL_JOB_ID, :P_ENVIRONMENT,
        :P_TRIGGERED_BY, 'RUNNING', :P_LOAD_WINDOW_START
    );

    SELECT MAX(RUN_ID) INTO :v_new_run_id
    FROM ELT_AUDIT.ELT_JOB_RUN_LOG
    WHERE JOB_NAME      = :P_JOB_NAME
      AND STATUS        = 'RUNNING'
      AND TRIGGERED_BY  = :P_TRIGGERED_BY
      AND START_TIME    >= DATEADD('minute', -5, SYSDATE());

    RETURN :v_new_run_id;
END;
$$;

COMMENT ON PROCEDURE ELT_AUDIT.sp_start_job_run(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,TIMESTAMP_NTZ)
    IS 'Opens an orchestration run record. Returns the RUN_ID - store this in a variable and pass to sp_end_job_run.';


/* ------------------------------------------------------------
   3.3  sp_end_job_run
   Closes an orchestration run record with final status and metrics.
   Call on BOTH success and failure paths.
   ------------------------------------------------------------ */
CREATE OR REPLACE PROCEDURE ELT_AUDIT.sp_end_job_run(
    P_RUN_ID            NUMBER,
    P_STATUS            VARCHAR,
    P_ROWS_EXTRACTED    NUMBER          DEFAULT 0,
    P_ROWS_INSERTED     NUMBER          DEFAULT 0,
    P_ROWS_UPDATED      NUMBER          DEFAULT 0,
    P_ROWS_DELETED      NUMBER          DEFAULT 0,
    P_ROWS_REJECTED     NUMBER          DEFAULT 0,
    P_ERROR_MESSAGE     VARCHAR         DEFAULT NULL,
    P_ERROR_CODE        VARCHAR         DEFAULT NULL,
    P_LOAD_WINDOW_END   TIMESTAMP_NTZ   DEFAULT NULL,
    P_NOTES             VARCHAR         DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE ELT_AUDIT.ELT_JOB_RUN_LOG
    SET
        STATUS              = :P_STATUS,
        END_TIME            = SYSDATE(),
        DURATION_SECONDS    = DATEDIFF('second', START_TIME, SYSDATE()),
        ROWS_EXTRACTED      = :P_ROWS_EXTRACTED,
        ROWS_INSERTED       = :P_ROWS_INSERTED,
        ROWS_UPDATED        = :P_ROWS_UPDATED,
        ROWS_DELETED        = :P_ROWS_DELETED,
        ROWS_REJECTED       = :P_ROWS_REJECTED,
        ERROR_MESSAGE       = :P_ERROR_MESSAGE,
        ERROR_CODE          = :P_ERROR_CODE,
        LOAD_WINDOW_END     = COALESCE(:P_LOAD_WINDOW_END, SYSDATE()),
        NOTES               = :P_NOTES
    WHERE RUN_ID = :P_RUN_ID;

    RETURN 'RUN_ID ' || :P_RUN_ID || ' closed with status: ' || :P_STATUS;
END;
$$;

COMMENT ON PROCEDURE ELT_AUDIT.sp_end_job_run(NUMBER,VARCHAR,NUMBER,NUMBER,NUMBER,NUMBER,NUMBER,VARCHAR,VARCHAR,TIMESTAMP_NTZ,VARCHAR)
    IS 'Closes an orchestration run record. Must be called on both success and failure paths. LOAD_WINDOW_END defaults to SYSDATE() if not supplied.';


/* ------------------------------------------------------------
   3.4  sp_start_step
   Opens a step-level record within an orchestration run.
   Returns the new STEP_LOG_ID.
   ------------------------------------------------------------ */
CREATE OR REPLACE PROCEDURE ELT_AUDIT.sp_start_step(
    P_RUN_ID        NUMBER,
    P_JOB_NAME      VARCHAR,
    P_STEP_NAME     VARCHAR,
    P_STEP_TYPE     VARCHAR     DEFAULT NULL,
    P_STEP_ORDER    NUMBER      DEFAULT NULL
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
DECLARE
    v_step_log_id NUMBER;
BEGIN
    INSERT INTO ELT_AUDIT.ELT_JOB_STEP_LOG (
        RUN_ID, JOB_NAME, STEP_NAME, STEP_TYPE, STEP_ORDER, STATUS
    ) VALUES (
        :P_RUN_ID, :P_JOB_NAME, :P_STEP_NAME, :P_STEP_TYPE, :P_STEP_ORDER, 'RUNNING'
    );

    SELECT MAX(STEP_LOG_ID) INTO :v_step_log_id
    FROM ELT_AUDIT.ELT_JOB_STEP_LOG
    WHERE RUN_ID    = :P_RUN_ID
      AND STEP_NAME = :P_STEP_NAME
      AND STATUS    = 'RUNNING'
      AND STEP_START_TIME >= DATEADD('minute', -2, SYSDATE());

    RETURN :v_step_log_id;
END;
$$;

COMMENT ON PROCEDURE ELT_AUDIT.sp_start_step(NUMBER,VARCHAR,VARCHAR,VARCHAR,NUMBER)
    IS 'Opens a step-level record within an orchestration run. Returns STEP_LOG_ID. Use to track individual sessions/tasks/components.';


/* ------------------------------------------------------------
   3.5  sp_end_step
   Closes a step-level record with final status and row counts.
   ------------------------------------------------------------ */
CREATE OR REPLACE PROCEDURE ELT_AUDIT.sp_end_step(
    P_STEP_LOG_ID       NUMBER,
    P_STATUS            VARCHAR,
    P_ROWS_PROCESSED    NUMBER  DEFAULT 0,
    P_ROWS_REJECTED     NUMBER  DEFAULT 0,
    P_ERROR_MESSAGE     VARCHAR DEFAULT NULL,
    P_ERROR_CODE        VARCHAR DEFAULT NULL,
    P_NOTES             VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE ELT_AUDIT.ELT_JOB_STEP_LOG
    SET
        STATUS              = :P_STATUS,
        STEP_END_TIME       = SYSDATE(),
        STEP_DURATION_SECS  = DATEDIFF('second', STEP_START_TIME, SYSDATE()),
        ROWS_PROCESSED      = :P_ROWS_PROCESSED,
        ROWS_REJECTED       = :P_ROWS_REJECTED,
        ERROR_MESSAGE       = :P_ERROR_MESSAGE,
        ERROR_CODE          = :P_ERROR_CODE,
        NOTES               = :P_NOTES
    WHERE STEP_LOG_ID = :P_STEP_LOG_ID;

    RETURN 'STEP_LOG_ID ' || :P_STEP_LOG_ID || ' closed with status: ' || :P_STATUS;
END;
$$;

COMMENT ON PROCEDURE ELT_AUDIT.sp_end_step(NUMBER,VARCHAR,NUMBER,NUMBER,VARCHAR,VARCHAR,VARCHAR)
    IS 'Closes a step-level record. Call on both success and failure paths of each step.';


/* ------------------------------------------------------------
   3.6  sp_log_error
   Appends a detailed error entry to ELT_ERROR_LOG.
   Can be called multiple times per run (e.g. one entry per failed row batch).
   ------------------------------------------------------------ */
CREATE OR REPLACE PROCEDURE ELT_AUDIT.sp_log_error(
    P_RUN_ID        NUMBER,
    P_JOB_NAME      VARCHAR,
    P_ERROR_MESSAGE VARCHAR,
    P_ERROR_CONTEXT VARCHAR    DEFAULT NULL,
    P_ERROR_CODE    VARCHAR    DEFAULT NULL,
    P_SEVERITY      VARCHAR    DEFAULT 'ERROR',
    P_STEP_LOG_ID   NUMBER     DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_error_log_id NUMBER;
BEGIN
    INSERT INTO ELT_AUDIT.ELT_ERROR_LOG (
        RUN_ID, JOB_NAME, STEP_LOG_ID, ERROR_MESSAGE,
        ERROR_CONTEXT, ERROR_CODE, SEVERITY
    ) VALUES (
        :P_RUN_ID, :P_JOB_NAME, :P_STEP_LOG_ID, :P_ERROR_MESSAGE,
        :P_ERROR_CONTEXT, :P_ERROR_CODE, :P_SEVERITY
    );

    SELECT MAX(ERROR_LOG_ID) INTO :v_error_log_id FROM ELT_AUDIT.ELT_ERROR_LOG
    WHERE RUN_ID = :P_RUN_ID AND LOGGED_AT >= DATEADD('second', -10, SYSDATE());

    RETURN 'Error logged. ERROR_LOG_ID: ' || :v_error_log_id;
END;
$$;

COMMENT ON PROCEDURE ELT_AUDIT.sp_log_error(NUMBER,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,NUMBER)
    IS 'Appends a detailed error record. Idempotent - safe to call multiple times per run for different errors.';


/* ------------------------------------------------------------
   3.7  sp_get_last_successful_window_end
   Helper for incremental loads. Returns the LOAD_WINDOW_END of
   the last successful run. Use this to set your next load window start.
   Returns '2000-01-01' if no prior successful run exists (first ever run).
   ------------------------------------------------------------ */
CREATE OR REPLACE PROCEDURE ELT_AUDIT.sp_get_last_successful_window_end(
    P_JOB_NAME      VARCHAR,
    P_ENVIRONMENT   VARCHAR     DEFAULT 'PROD'
)
RETURNS TIMESTAMP_NTZ
LANGUAGE SQL
AS
$$
DECLARE
    v_last_end TIMESTAMP_NTZ;
BEGIN
    SELECT COALESCE(MAX(LOAD_WINDOW_END), '2000-01-01'::TIMESTAMP_NTZ)
    INTO   :v_last_end
    FROM   ELT_AUDIT.ELT_JOB_RUN_LOG
    WHERE  JOB_NAME     = :P_JOB_NAME
      AND  STATUS       = 'SUCCESS'
      AND  ENVIRONMENT  = :P_ENVIRONMENT;

    RETURN :v_last_end;
END;
$$;

COMMENT ON PROCEDURE ELT_AUDIT.sp_get_last_successful_window_end(VARCHAR,VARCHAR)
    IS 'Returns the LOAD_WINDOW_END of the last successful run. Use as the LOAD_WINDOW_START for incremental loads. Returns 2000-01-01 if no history exists.';


-- ============================================================
-- SECTION 4: VIEWS
-- ============================================================

/* ------------------------------------------------------------
   4.1  VW_RECENT_RUN_HISTORY
   Latest run result for each job, across all tools.
   Good for a daily health dashboard.
   ------------------------------------------------------------ */
CREATE OR REPLACE VIEW ELT_AUDIT.VW_RECENT_RUN_HISTORY AS
SELECT
    r.RUN_ID,
    r.JOB_NAME,
    c.SOURCE_SYSTEM,
    c.TARGET_TABLE,
    c.OWNER_TEAM,
    r.TOOL_NAME,
    r.ENVIRONMENT,
    r.TRIGGERED_BY,
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
    r.ERROR_MESSAGE,
    r.NOTES
FROM       ELT_AUDIT.ELT_JOB_RUN_LOG  r
LEFT JOIN  ELT_AUDIT.ELT_JOB_CATALOG  c ON c.JOB_NAME = r.JOB_NAME
ORDER BY   r.START_TIME DESC;

COMMENT ON VIEW ELT_AUDIT.VW_RECENT_RUN_HISTORY IS 'All run history newest-first, joined with catalog metadata. Filter by ENVIRONMENT and STATUS for dashboards.';


/* ------------------------------------------------------------
   4.2  VW_JOB_SUMMARY
   One row per job showing last-run metrics.
   ------------------------------------------------------------ */
CREATE OR REPLACE VIEW ELT_AUDIT.VW_JOB_SUMMARY AS
SELECT
    c.JOB_NAME,
    c.SOURCE_SYSTEM,
    c.TARGET_TABLE,
    c.LOAD_TYPE,
    c.OWNER_TEAM,
    c.IS_ACTIVE,
    -- Latest run
    latest.TOOL_NAME            AS LAST_TOOL_NAME,
    latest.STATUS               AS LAST_STATUS,
    latest.START_TIME           AS LAST_RUN_START,
    latest.DURATION_SECONDS     AS LAST_DURATION_SECONDS,
    latest.ROWS_INSERTED        AS LAST_ROWS_INSERTED,
    latest.ROWS_REJECTED        AS LAST_ROWS_REJECTED,
    latest.LOAD_WINDOW_END      AS LAST_WINDOW_END,
    -- Aggregate stats (PROD only)
    stats.TOTAL_RUNS,
    stats.SUCCESS_RUNS,
    stats.FAILED_RUNS,
    ROUND(stats.SUCCESS_RUNS / NULLIF(stats.TOTAL_RUNS, 0) * 100, 1) AS SUCCESS_RATE_PCT,
    stats.AVG_DURATION_SECONDS,
    stats.AVG_ROWS_INSERTED
FROM ELT_AUDIT.ELT_JOB_CATALOG c
LEFT JOIN LATERAL (
    -- Most recent run for this job
    SELECT TOOL_NAME, STATUS, START_TIME, DURATION_SECONDS,
           ROWS_INSERTED, ROWS_REJECTED, LOAD_WINDOW_END
    FROM   ELT_AUDIT.ELT_JOB_RUN_LOG
    WHERE  JOB_NAME = c.JOB_NAME
    ORDER  BY START_TIME DESC
    LIMIT  1
) latest ON TRUE
LEFT JOIN LATERAL (
    -- Aggregate stats across all PROD runs
    SELECT
        COUNT(*)                        AS TOTAL_RUNS,
        SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESS_RUNS,
        SUM(CASE WHEN STATUS = 'FAILED'  THEN 1 ELSE 0 END) AS FAILED_RUNS,
        ROUND(AVG(DURATION_SECONDS), 0) AS AVG_DURATION_SECONDS,
        ROUND(AVG(ROWS_INSERTED), 0)    AS AVG_ROWS_INSERTED
    FROM   ELT_AUDIT.ELT_JOB_RUN_LOG
    WHERE  JOB_NAME    = c.JOB_NAME
      AND  ENVIRONMENT = 'PROD'
) stats ON TRUE
ORDER BY c.JOB_NAME;

COMMENT ON VIEW ELT_AUDIT.VW_JOB_SUMMARY IS 'One row per registered job with last-run status and aggregate success rates. Use for pipeline health monitoring.';


/* ------------------------------------------------------------
   4.3  VW_FAILED_RUNS
   All failed runs in the last 7 days with error detail.
   ------------------------------------------------------------ */
CREATE OR REPLACE VIEW ELT_AUDIT.VW_FAILED_RUNS AS
SELECT
    r.RUN_ID,
    r.JOB_NAME,
    c.OWNER_TEAM,
    r.TOOL_NAME,
    r.ENVIRONMENT,
    r.START_TIME,
    r.END_TIME,
    r.DURATION_SECONDS,
    r.ERROR_MESSAGE,
    r.ERROR_CODE,
    r.NOTES,
    -- Step that failed (if step logging is used)
    s.STEP_NAME         AS FAILED_STEP_NAME,
    s.STEP_TYPE         AS FAILED_STEP_TYPE,
    s.ERROR_MESSAGE     AS STEP_ERROR_MESSAGE
FROM       ELT_AUDIT.ELT_JOB_RUN_LOG  r
LEFT JOIN  ELT_AUDIT.ELT_JOB_CATALOG  c ON c.JOB_NAME = r.JOB_NAME
LEFT JOIN  ELT_AUDIT.ELT_JOB_STEP_LOG s
           ON  s.RUN_ID = r.RUN_ID
           AND s.STATUS = 'FAILED'
WHERE  r.STATUS IN ('FAILED', 'WARNING')
  AND  r.START_TIME >= DATEADD('day', -7, SYSDATE())
ORDER BY r.START_TIME DESC;

COMMENT ON VIEW ELT_AUDIT.VW_FAILED_RUNS IS 'Failed and warning runs from the last 7 days, joined with step-level failure detail where available.';


/* ------------------------------------------------------------
   4.4  VW_CURRENTLY_RUNNING
   Any job that has a RUNNING status older than expected.
   A run stuck in RUNNING is a sign the end procedure was not called.
   ------------------------------------------------------------ */
CREATE OR REPLACE VIEW ELT_AUDIT.VW_CURRENTLY_RUNNING AS
SELECT
    r.RUN_ID,
    r.JOB_NAME,
    r.TOOL_NAME,
    r.ENVIRONMENT,
    r.TRIGGERED_BY,
    r.START_TIME,
    DATEDIFF('minute', r.START_TIME, SYSDATE()) AS MINUTES_RUNNING,
    c.SCHEDULE,
    c.OWNER_TEAM
FROM      ELT_AUDIT.ELT_JOB_RUN_LOG r
LEFT JOIN ELT_AUDIT.ELT_JOB_CATALOG c ON c.JOB_NAME = r.JOB_NAME
WHERE  r.STATUS = 'RUNNING'
ORDER BY r.START_TIME ASC;

COMMENT ON VIEW ELT_AUDIT.VW_CURRENTLY_RUNNING IS 'All jobs currently in RUNNING status. Any row here older than expected duration may be an orphaned run needing manual closure.';


/* ------------------------------------------------------------
   4.5  VW_STEP_DETAIL
   Granular step breakdown for a given run.
   Most useful when debugging: join to a RUN_ID to see the full step sequence.
   ------------------------------------------------------------ */
CREATE OR REPLACE VIEW ELT_AUDIT.VW_STEP_DETAIL AS
SELECT
    s.STEP_LOG_ID,
    s.RUN_ID,
    s.JOB_NAME,
    s.STEP_ORDER,
    s.STEP_NAME,
    s.STEP_TYPE,
    s.STATUS,
    s.STEP_START_TIME,
    s.STEP_END_TIME,
    s.STEP_DURATION_SECS,
    s.ROWS_PROCESSED,
    s.ROWS_REJECTED,
    s.ERROR_MESSAGE,
    s.ERROR_CODE,
    s.NOTES,
    -- Parent run context
    r.TOOL_NAME,
    r.ENVIRONMENT,
    r.START_TIME        AS RUN_START_TIME
FROM      ELT_AUDIT.ELT_JOB_STEP_LOG  s
JOIN      ELT_AUDIT.ELT_JOB_RUN_LOG   r ON r.RUN_ID = s.RUN_ID
ORDER BY  s.RUN_ID DESC, s.STEP_ORDER ASC NULLS LAST, s.STEP_START_TIME ASC;

COMMENT ON VIEW ELT_AUDIT.VW_STEP_DETAIL IS 'Step-by-step breakdown for any run. Filter by RUN_ID to see the full sequence for a specific execution.';


-- ============================================================
-- SECTION 5: EXAMPLE USAGE
-- ============================================================

/* ---- Example: Register a pipeline ---- 
CALL ELT_AUDIT.sp_register_job(
    'LOAD_SALES_FACT_DAILY',          -- JOB_NAME  (use this exact string everywhere)
    'SALESFORCE',                      -- SOURCE_SYSTEM
    'SALES_DW',                        -- TARGET_SCHEMA
    'SALES_DW.FACT_SALES',             -- TARGET_TABLE
    'INCREMENTAL',                     -- LOAD_TYPE
    'Data Engineering',                -- OWNER_TEAM
    'Daily 02:00 UTC',                 -- SCHEDULE
    'Daily incremental from Salesforce opportunities'  -- NOTES
);

---- Example: Run with Informatica ----
CALL ELT_AUDIT.sp_start_job_run(
    'LOAD_SALES_FACT_DAILY',
    'INFORMATICA',                     -- TOOL_NAME - just change this when you switch tools
    'WF_LOAD_SALES_20240115_001',      -- TOOL_JOB_ID: Informatica workflow run ID
    'PROD',
    'svc_informatica'
);
-- Store the returned NUMBER as your RUN_ID variable

---- Example: Start a step ----
CALL ELT_AUDIT.sp_start_step(
    <RUN_ID>,
    'LOAD_SALES_FACT_DAILY',
    'EXTRACT_FROM_SALESFORCE',
    'EXTRACT',
    1
);

---- Example: End a step successfully ----
CALL ELT_AUDIT.sp_end_step(<STEP_LOG_ID>, 'SUCCESS', 15000, 0);

---- Example: End the run successfully ----
CALL ELT_AUDIT.sp_end_job_run(
    <RUN_ID>,
    'SUCCESS',
    15000,   -- rows extracted
    14985,   -- rows inserted
    0,       -- rows updated
    0,       -- rows deleted
    15,      -- rows rejected
    NULL, NULL,
    SYSDATE(),
    'Completed normally'
);

---- Example: When you switch to Matillion ----
-- Nothing changes in the database. Just pass 'MATILLION' as TOOL_NAME:
CALL ELT_AUDIT.sp_start_job_run(
    'LOAD_SALES_FACT_DAILY',
    'MATILLION',                       -- <-- only this changes
    'PROD-LOAD_SALES',
    'PROD',
    'svc_matillion'
);
-- Full run history from Informatica is preserved. TOOL_NAME column shows both.

---- Example: Incremental load - get last successful window end ----
CALL ELT_AUDIT.sp_get_last_successful_window_end('LOAD_SALES_FACT_DAILY', 'PROD');
-- Returns the LOAD_WINDOW_END of the last successful PROD run.
-- Use the result as the WHERE clause filter in your source query.

---- Example: Orphaned run cleanup ----
UPDATE ELT_AUDIT.ELT_JOB_RUN_LOG
SET    STATUS    = 'FAILED',
       END_TIME  = SYSDATE(),
       NOTES     = 'Manually closed - orphaned run, sp_end_job_run was not called'
WHERE  STATUS    = 'RUNNING'
  AND  RUN_ID    = <RUN_ID>;
*/
