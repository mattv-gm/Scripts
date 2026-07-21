-- Email notification integration and stored procedure for pipeline alerts

/*
  SETUP INSTRUCTIONS:
  1. Replace placeholder emails in ALLOWED_RECIPIENTS with your team's verified email addresses.
     Keep this list in sync with v_default_recipients inside send_pipeline_alert() below —
     they are two independent copies of the same list.
  2. Each recipient must verify their email in Snowsight (Profile > Email) or via SYSTEM$START_USER_EMAIL_VERIFICATION.
  3. Run the CREATE NOTIFICATION INTEGRATION with a role that has CREATE INTEGRATION privilege (e.g., ACCOUNTADMIN).
  4. Replace ANALYTICS_DB below with your actual database name.
  5. Grant USAGE on the integration and EXECUTE on the procedure to roles that run your pipelines.
*/

----------------------------------------------------------------------
-- 0. Schema setup — dedicated schema, kept independent of the ELT audit
--    framework in SQL/ELT_Log.sql so this can be deployed on its own.
----------------------------------------------------------------------
USE DATABASE ANALYTICS_DB;

CREATE SCHEMA IF NOT EXISTS PIPELINE_ALERTS
  COMMENT = 'Pipeline email alerting: notification integration, alert log, send_pipeline_alert().';

USE SCHEMA PIPELINE_ALERTS;

----------------------------------------------------------------------
-- 1. Create the email notification integration
----------------------------------------------------------------------
CREATE OR REPLACE NOTIFICATION INTEGRATION pipeline_email_alerts_int
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = (
    'team_member_1@example.com',
    'team_member_2@example.com'
  )
  DEFAULT_SUBJECT = 'Pipeline Alert';

----------------------------------------------------------------------
-- 2. Create the alert log table
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_alert_log (
    alert_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    pipeline_name   VARCHAR NOT NULL,
    status          VARCHAR NOT NULL,
    severity        VARCHAR NOT NULL,
    message         VARCHAR,
    recipients      VARCHAR,
    -- Whether SYSTEM$SEND_EMAIL actually succeeded — a logged row does NOT by
    -- itself mean the email was delivered; check delivery_status.
    delivery_status VARCHAR NOT NULL DEFAULT 'SENT'  -- SENT | FAILED
                        CHECK (delivery_status IN ('SENT', 'FAILED')),
    error_message   VARCHAR,  -- populated when delivery_status = 'FAILED'
    sent_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    sent_by         VARCHAR DEFAULT CURRENT_USER()
);

----------------------------------------------------------------------
-- 3. Create the stored procedure pipelines can call
----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE send_pipeline_alert(
    p_pipeline_name VARCHAR,
    p_status        VARCHAR,              -- 'SUCCESS', 'FAILURE', 'WARNING', etc. — free text, not a fixed enum
    p_message       VARCHAR,
    p_severity      VARCHAR DEFAULT 'INFO',  -- 'INFO', 'WARNING', 'ERROR'
    p_recipients    VARCHAR DEFAULT NULL     -- comma-separated override; defaults to v_default_recipients below
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    err_invalid_severity EXCEPTION (-20001, 'send_pipeline_alert: p_severity must be one of INFO, WARNING, ERROR.');

    -- Keep this in sync with ALLOWED_RECIPIENTS on pipeline_email_alerts_int
    -- above — they are two independent copies of the same list.
    v_default_recipients VARCHAR DEFAULT 'team_member_1@example.com, team_member_2@example.com';

    v_subject          VARCHAR;
    v_body             VARCHAR;
    v_recipients       VARCHAR;
    v_integration      VARCHAR DEFAULT 'pipeline_email_alerts_int';
    v_severity_tag     VARCHAR;
    v_delivery_status  VARCHAR;
    v_error_message    VARCHAR DEFAULT NULL;
BEGIN
    -- Normalize + validate severity (drives both the subject styling below
    -- and the CASE branch, so unlike p_status this one is a real enum)
    v_severity_tag := UPPER(:p_severity);

    IF (v_severity_tag NOT IN ('INFO', 'WARNING', 'ERROR')) THEN
        RAISE err_invalid_severity;
    END IF;

    v_recipients := COALESCE(:p_recipients, :v_default_recipients);

    -- Build subject line based on severity
    CASE v_severity_tag
        WHEN 'ERROR' THEN
            v_subject := '🚨 [ERROR] Pipeline Alert: ' || :p_pipeline_name;
        WHEN 'WARNING' THEN
            v_subject := '⚠️ [WARNING] Pipeline Alert: ' || :p_pipeline_name;
        ELSE
            v_subject := 'ℹ️ [INFO] Pipeline Alert: ' || :p_pipeline_name;
    END CASE;

    -- Build email body
    v_body := '--- Pipeline Alert ---' || '\n\n' ||
              'Pipeline:  ' || :p_pipeline_name || '\n' ||
              'Status:    ' || UPPER(:p_status) || '\n' ||
              'Severity:  ' || :v_severity_tag || '\n' ||
              'Timestamp: ' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS TZH:TZM') || '\n\n' ||
              'Message:\n' || :p_message || '\n\n' ||
              '--- End of Alert ---';

    -- Attempt delivery in its own block. A failed send must not raise out of
    -- this procedure — a notification problem shouldn't be able to fail the
    -- pipeline that called it — so it's caught here and recorded instead.
    BEGIN
        CALL SYSTEM$SEND_EMAIL(
            :v_integration,
            :v_recipients,
            :v_subject,
            :v_body
        );
        v_delivery_status := 'SENT';
    EXCEPTION
        WHEN OTHER THEN
            v_delivery_status := 'FAILED';
            v_error_message   := SQLERRM;
    END;

    -- Log the alert with the real outcome — never insert until we actually
    -- know whether the email went out.
    INSERT INTO pipeline_alert_log (
        pipeline_name, status, severity, message, recipients,
        delivery_status, error_message
    )
    VALUES (
        :p_pipeline_name, UPPER(:p_status), :v_severity_tag, :p_message, :v_recipients,
        :v_delivery_status, :v_error_message
    );

    IF (v_delivery_status = 'FAILED') THEN
        RETURN 'Alert logged but email delivery FAILED for ' || :p_pipeline_name || ': ' || :v_error_message;
    END IF;

    RETURN 'Alert sent successfully: ' || :v_severity_tag || ' - ' || :p_pipeline_name;
END;
$$
COMMENT = 'Sends a pipeline alert email and logs it. Delivery failures are captured in pipeline_alert_log, not raised, so a notification problem cannot fail the calling pipeline.';

----------------------------------------------------------------------
-- 4. Grant permissions (adjust role names to your environment)
----------------------------------------------------------------------
-- GRANT USAGE ON INTEGRATION pipeline_email_alerts_int TO ROLE my_pipeline_role;
-- GRANT USAGE ON PROCEDURE send_pipeline_alert(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE my_pipeline_role;

----------------------------------------------------------------------
-- 5. Example usage from a pipeline
----------------------------------------------------------------------
/*
-- Success notification
CALL send_pipeline_alert(
    'daily_sales_load',
    'SUCCESS',
    'Loaded 1,245,678 records in 3m 22s. No errors detected.',
    'INFO'
);

-- Failure notification
CALL send_pipeline_alert(
    'daily_sales_load',
    'FAILURE',
    'Stage file missing: @raw_stage/sales/2026-07-20/. Pipeline halted.',
    'ERROR'
);

-- Warning notification
CALL send_pipeline_alert(
    'daily_sales_load',
    'PARTIAL SUCCESS',
    '12 of 15 files loaded. 3 files had schema mismatches and were skipped.',
    'WARNING'
);

-- Query the alert log
SELECT * FROM pipeline_alert_log
  WHERE pipeline_name = 'daily_sales_load'
  ORDER BY sent_at DESC
  LIMIT 20;

-- Summary of alerts by severity in the last 7 days
SELECT severity, COUNT(*) AS alert_count
  FROM pipeline_alert_log
  WHERE sent_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  GROUP BY severity
  ORDER BY alert_count DESC;

-- Alerts that were logged but never actually delivered (needs attention —
-- e.g. an unverified recipient or a disabled integration)
SELECT *
  FROM pipeline_alert_log
  WHERE delivery_status = 'FAILED'
  ORDER BY sent_at DESC;

-- Override the default recipient list for one call
CALL send_pipeline_alert(
    'daily_sales_load',
    'FAILURE',
    'Stage file missing: @raw_stage/sales/2026-07-20/. Pipeline halted.',
    'ERROR',
    'oncall@example.com, data-eng-lead@example.com'
);
*/
