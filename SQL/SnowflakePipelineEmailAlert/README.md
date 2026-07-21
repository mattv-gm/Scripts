# Pipeline Email Alerts

`pipeline_email_alerts.sql` gives your pipelines a single stored procedure to call whenever they want to notify a human by email — success confirmations, warnings, failures — with every alert logged and its actual delivery outcome tracked. It's independent of the ELT audit framework in [`SQL/ETLAuditLog_EntityRef/ELT_Log.sql`](../ETLAuditLog_EntityRef/ELT_Log.sql) and can be deployed on its own.

## How it works

| Object | Purpose |
|---|---|
| `pipeline_email_alerts_int` | A Snowflake email `NOTIFICATION INTEGRATION`. Snowflake only allows sending email to addresses on this integration's `ALLOWED_RECIPIENTS` list, and each address must be verified by its owner first. |
| `pipeline_alert_log` | One row per alert attempt — pipeline name, environment, execution ID, status, severity, message, recipients, and (critically) whether the email actually sent. |
| `send_pipeline_alert(...)` | The one procedure your pipelines call. Builds a formatted email, attempts delivery, and logs the outcome either way. |

A logged row is not proof of delivery — check `delivery_status`.

## `send_pipeline_alert` parameters

```sql
CALL send_pipeline_alert(
    p_pipeline_name,          -- VARCHAR, required. Name of the pipeline/job the alert is about.
    p_status,                 -- VARCHAR, required. Free text: 'SUCCESS', 'FAILURE', 'PARTIAL SUCCESS', etc.
    p_message,                -- VARCHAR, required. The body of the alert — what happened.
    p_severity,               -- VARCHAR, default 'INFO'. Must be INFO, WARNING, or ERROR — drives the subject line/emoji.
    p_recipients,             -- VARCHAR, default NULL. Comma-separated email addresses. NULL = use the default list
                              --   (which must be a subset of ALLOWED_RECIPIENTS on the integration).
    p_environment,            -- VARCHAR, default 'PROD'. Shown in the subject tag and email body so PROD alerts
                              --   are never confused with DEV/UAT noise.
    p_execution_id            -- VARCHAR, default NULL. Any run/session/task ID you want tied to this alert — e.g.
                              --   the RUN_ID/PIPELINE_RUN_ID from the ELT audit framework, or your ELT tool's own ID.
);
```

Returns a `VARCHAR` describing what happened — either `'Alert sent successfully: ...'` or `'Alert logged but email delivery FAILED for ...: <reason>'`. **The procedure itself never raises an error just because the email failed to send** — a notification problem is captured, not propagated, so it can't fail the pipeline that called it. Check the return string or query `pipeline_alert_log` if you need to react to delivery failures.

`p_severity` is validated (must be `INFO`/`WARNING`/`ERROR`) since it drives branching logic; `p_status` and `p_environment` are intentionally free text since pipelines/accounts report these in all kinds of ways — both are just normalized to uppercase for consistent display and filtering.

## Typical usage

```sql
-- On success
CALL send_pipeline_alert(
    'daily_sales_load', 'SUCCESS',
    'Loaded 1,245,678 records in 3m 22s. No errors detected.',
    'INFO'
);

-- On failure — goes to the default recipient list
CALL send_pipeline_alert(
    'daily_sales_load', 'FAILURE',
    'Stage file missing: @raw_stage/sales/2026-07-20/. Pipeline halted.',
    'ERROR'
);

-- On failure, routed to on-call instead of the default list
CALL send_pipeline_alert(
    'daily_sales_load', 'FAILURE',
    'Stage file missing: @raw_stage/sales/2026-07-20/. Pipeline halted.',
    'ERROR',
    'oncall@example.com, data-eng-lead@example.com'
);

-- Tagging the environment and tying it back to a specific run
CALL send_pipeline_alert(
    'daily_sales_load', 'FAILURE',
    'Stage file missing: @raw_stage/sales/2026-07-20/. Pipeline halted.',
    'ERROR',
    NULL,           -- p_recipients: use the default list
    'PROD',         -- p_environment
    'RUN_ID=4821'   -- p_execution_id
);
```

## Monitoring queries

```sql
-- Recent alerts for one pipeline
SELECT * FROM pipeline_alert_log
  WHERE pipeline_name = 'daily_sales_load'
  ORDER BY sent_at DESC
  LIMIT 20;

-- Alert volume by severity, last 7 days
SELECT severity, COUNT(*) AS alert_count
  FROM pipeline_alert_log
  WHERE sent_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  GROUP BY severity
  ORDER BY alert_count DESC;

-- Alerts that never actually delivered — needs attention
SELECT * FROM pipeline_alert_log
  WHERE delivery_status = 'FAILED'
  ORDER BY sent_at DESC;

-- All PROD alerts tied to one specific execution
SELECT * FROM pipeline_alert_log
  WHERE environment = 'PROD' AND execution_id = 'RUN_ID=4821'
  ORDER BY sent_at DESC;
```

## Deployment

1. **Replace `ANALYTICS_DB`** near the top of `pipeline_email_alerts.sql` with your real database name. The script creates its own `PIPELINE_ALERTS` schema there.
2. **Replace the placeholder emails** in `ALLOWED_RECIPIENTS` (on the integration) — and keep `v_default_recipients` inside `send_pipeline_alert` in sync with it. These are two independent copies of the same list; nothing keeps them in sync automatically.
3. Each recipient address must **verify their email** in Snowsight (Profile → Email) or via `SYSTEM$START_USER_EMAIL_VERIFICATION` before they can receive anything.
4. Run the whole file with a role that has `CREATE INTEGRATION` privilege (e.g. `ACCOUNTADMIN`) — notification integrations are account-level objects.
5. Uncomment and adjust the `GRANT` statements near the bottom so the roles that run your pipelines can actually call the procedure:
   ```sql
   GRANT USAGE ON INTEGRATION pipeline_email_alerts_int TO ROLE my_pipeline_role;
   GRANT USAGE ON PROCEDURE send_pipeline_alert(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE my_pipeline_role;
   ```

## Design notes

- **Delivery failures are caught, not raised.** `SYSTEM$SEND_EMAIL` is called inside a nested `BEGIN...EXCEPTION...END` block; a failure (unverified recipient, disabled integration, etc.) is recorded via `SQLERRM` into `delivery_status`/`error_message` rather than bubbling up as an unhandled error. A notification side-effect failing should never be able to fail the actual pipeline run.
- **The log row is only ever written once, after the send attempt** — so `delivery_status` always reflects what really happened, never an optimistic guess made before the email was tried.
- **`RAISE` in Snowflake Scripting only accepts a pre-declared exception name** (no inline `'message %', arg` interpolation) — same constraint documented in [`SQL/ETLAuditLog_EntityRef/README.md`](../ETLAuditLog_EntityRef/README.md#design-decisions-worth-knowing) for the ELT audit framework. `p_severity` validation uses a declared `EXCEPTION` object accordingly.
- **`p_execution_id` is a free-form VARCHAR**, not a foreign key to the ELT audit framework's `RUN_ID`/`PIPELINE_RUN_ID` — this file is deliberately independent of that framework, so it accepts any identifier string (a run ID, a session ID, a Taskflow instance ID) rather than assuming that framework is deployed.
