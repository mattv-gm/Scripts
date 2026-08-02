/*
================================================================================
  BIGQUERY → SNOWFLAKE CONNECTOR
  A reusable, parameterized "connector" — call it like you would any
  integration tool's BigQuery connector, but it runs natively inside
  Snowflake as a Python stored procedure.

  SNOWFLAKE-NATIVE ABILITIES THIS USES
  ───────────────────────────────────────
  - EXTERNAL ACCESS INTEGRATION: outbound HTTPS to Google's API from inside
    the normally-sandboxed Python runtime — no external orchestrator, no
    egress proxy to manage.
  - SECRET: the GCP service account key is pulled at runtime via
    _snowflake.get_generic_secret_string() and never appears in code, query
    text, or query history.
  - Snowpark write_pandas(): loads extracted rows straight into a Snowflake
    table from memory (PUT + COPY INTO under the hood) — no intermediate
    stage, S3 bucket, or file to manage.
  - Runs on a Snowflake virtual warehouse, so extraction/load throughput
    scales with warehouse size like any other Snowflake workload.
  - Plugs into the existing tool-agnostic ELT_AUDIT framework
    (SQL/ETLAuditLog_EntityRef/ELT_Log.sql) — every call is logged as a job
    run with TOOL_NAME = 'PYTHON_BQ_CONNECTOR', same as Informatica/Matillion
    runs, so it shows up in VW_JOB_SUMMARY / VW_FAILED_RUNS for free.

  PREREQUISITES
  ───────────────
  Run 00_setup_external_access.sql first. It creates:
    ANALYTICS_DB.BQ_CONNECTOR.bq_network_rule
    ANALYTICS_DB.BQ_CONNECTOR.bq_connector_secret
    ANALYTICS_DB.BQ_CONNECTOR.bq_external_access_integration
  Also assumes SQL/ETLAuditLog_EntityRef/ELT_Log.sql has been run — the
  connector calls sp_start_job_run / sp_end_job_run / sp_log_error in
  ANALYTICS_DB.ELT_AUDIT. Audit logging is best-effort: if the job hasn't
  been registered with sp_register_job, or ELT_AUDIT isn't deployed, the
  connector still runs the actual BigQuery pull — it just won't show up in
  the audit views.

  PARAMETERS (think of these as the connector's UI fields)
  ────────────────────────────────────────────────────────
  p_job_name            Job name registered via sp_register_job. Used for
                         audit logging only — the pull still runs if unset
                         or unregistered.
  p_bq_project           Source GCP project ID.
  p_bq_query             Full BigQuery SQL to run. Takes precedence over
                          p_bq_dataset/p_bq_table if both are given. May
                          contain the literal placeholders {load_window_start}
                          and {load_window_end}, substituted from those
                          parameters.
  p_bq_dataset/p_bq_table Used to build a plain `SELECT * FROM
                          project.dataset.table` when p_bq_query is NULL.
  p_target_database/
  p_target_schema/
  p_target_table          Destination in Snowflake. Auto-created on first
                          load with a schema inferred from BigQuery.
  p_write_mode            APPEND (default) | OVERWRITE | MERGE.
                          OVERWRITE truncates and replaces the target.
                          MERGE upserts on p_merge_keys via a staging table.
  p_merge_keys            Comma-separated column names. Required for MERGE.
  p_watermark_column      BigQuery column used for incremental filtering
                          when p_bq_query is NULL — filtered to the
                          [p_load_window_start, p_load_window_end) range.
  p_load_window_start/
  p_load_window_end       ISO-8601 strings (e.g. '2026-07-20T00:00:00').
                          Pass pipeline-controlled timestamps here, not raw
                          end-user input — they are interpolated into the
                          BigQuery SQL text and into the {placeholders}
                          above.
  p_batch_size            Rows per page pulled from BigQuery and flushed to
                          Snowflake. Keeps memory bounded on large tables.
                          Default 100000.
  p_secret_alias          Which bound secret alias to use for GCP
                          credentials. Default 'bq_service_account' — see
                          "MULTIPLE BQ CONNECTIONS" at the bottom of this
                          file to add more.
  p_environment           'PROD' | 'UAT' | 'DEV' — passed through to the
                          audit log.
  p_triggered_by          Defaults to CURRENT_USER() if not supplied.

  SETUP ORDER
  ───────────
  Run 00_setup_external_access.sql, then this file.
================================================================================
*/

USE DATABASE ANALYTICS_DB;
USE SCHEMA BQ_CONNECTOR;


CREATE OR REPLACE PROCEDURE sp_bq_to_snowflake(
    p_job_name              VARCHAR,
    p_bq_project            VARCHAR,
    p_bq_query              VARCHAR DEFAULT NULL,
    p_bq_dataset            VARCHAR DEFAULT NULL,
    p_bq_table              VARCHAR DEFAULT NULL,
    p_target_database       VARCHAR DEFAULT NULL,
    p_target_schema         VARCHAR DEFAULT NULL,
    p_target_table          VARCHAR DEFAULT NULL,
    p_write_mode            VARCHAR DEFAULT 'APPEND',
    p_merge_keys            VARCHAR DEFAULT NULL,
    p_watermark_column      VARCHAR DEFAULT NULL,
    p_load_window_start     VARCHAR DEFAULT NULL,
    p_load_window_end       VARCHAR DEFAULT NULL,
    p_batch_size            NUMBER  DEFAULT 100000,
    p_secret_alias          VARCHAR DEFAULT 'bq_service_account',
    p_environment           VARCHAR DEFAULT 'PROD',
    p_triggered_by          VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'google-cloud-bigquery', 'google-auth', 'pandas')
EXTERNAL_ACCESS_INTEGRATIONS = (bq_external_access_integration)
SECRETS = ('bq_service_account' = bq_connector_secret)
HANDLER = 'run'
AS
$$
import json
from datetime import datetime

import _snowflake
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

TOOL_NAME = 'PYTHON_BQ_CONNECTOR'
VALID_WRITE_MODES = ('APPEND', 'OVERWRITE', 'MERGE')
AUDIT_SCHEMA = 'ELT_AUDIT'  # ANALYTICS_DB.ELT_AUDIT — see SQL/ETLAuditLog_EntityRef


def run(session, p_job_name, p_bq_project, p_bq_query, p_bq_dataset, p_bq_table,
        p_target_database, p_target_schema, p_target_table,
        p_write_mode, p_merge_keys, p_watermark_column,
        p_load_window_start, p_load_window_end, p_batch_size,
        p_secret_alias, p_environment, p_triggered_by):

    write_mode = (p_write_mode or 'APPEND').upper()
    if write_mode not in VALID_WRITE_MODES:
        raise ValueError(f"p_write_mode must be one of {VALID_WRITE_MODES}, got '{p_write_mode}'")
    if write_mode == 'MERGE' and not p_merge_keys:
        raise ValueError("p_merge_keys is required when p_write_mode = 'MERGE'")
    if not p_bq_query and not (p_bq_dataset and p_bq_table):
        raise ValueError("Provide either p_bq_query, or both p_bq_dataset and p_bq_table")
    if not (p_target_database and p_target_schema and p_target_table):
        raise ValueError("p_target_database, p_target_schema, and p_target_table are all required")

    triggered_by = p_triggered_by or session.sql("SELECT CURRENT_USER()").collect()[0][0]

    run_id = _start_audit_run(
        session, p_job_name, p_environment, triggered_by,
        p_load_window_start, p_load_window_end,
    )

    try:
        query_text = _build_query(
            p_bq_project, p_bq_query, p_bq_dataset, p_bq_table,
            p_watermark_column, p_load_window_start, p_load_window_end,
        )

        bq_client = _build_bq_client(p_bq_project, p_secret_alias)
        query_job = bq_client.query(query_text)
        result_iter = query_job.result(page_size=int(p_batch_size))

        load_table = p_target_table if write_mode != 'MERGE' else f"{p_target_table}_STG_{run_id or 'TMP'}"

        rows_extracted = 0
        first_batch = True
        for page in result_iter.pages:
            records = [dict(row.items()) for row in page]
            if not records:
                continue
            df = pd.DataFrame.from_records(records)
            rows_extracted += len(df)

            success, _, _, _ = session.write_pandas(
                df,
                table_name=load_table,
                database=p_target_database,
                schema=p_target_schema,
                auto_create_table=True,
                overwrite=(first_batch and write_mode == 'OVERWRITE'),
                quote_identifiers=True,
            )
            if not success:
                raise RuntimeError(f"write_pandas reported failure writing a batch to {load_table}")
            first_batch = False

        if rows_extracted == 0 and write_mode == 'OVERWRITE':
            # No source rows this run — still truncate the target so
            # downstream consumers see an empty table, not a stale one.
            empty_cols = [f.name for f in result_iter.schema]
            session.write_pandas(
                pd.DataFrame(columns=empty_cols),
                table_name=p_target_table, database=p_target_database, schema=p_target_schema,
                auto_create_table=True, overwrite=True, quote_identifiers=True,
            )

        if write_mode == 'MERGE' and rows_extracted > 0:
            _merge_staging_into_target(
                session, p_target_database, p_target_schema,
                load_table, p_target_table, p_merge_keys,
            )
        if write_mode == 'MERGE':
            session.sql(f'DROP TABLE IF EXISTS "{p_target_database}"."{p_target_schema}"."{load_table}"').collect()

        _end_audit_run(session, run_id, 'SUCCESS', rows_extracted, write_mode)

        return {
            "status": "SUCCESS",
            "run_id": run_id,
            "rows_extracted": rows_extracted,
            "write_mode": write_mode,
            "target_table": f"{p_target_database}.{p_target_schema}.{p_target_table}",
        }

    except Exception as e:
        _log_audit_error(session, run_id, p_job_name, str(e))
        _end_audit_run(session, run_id, 'FAILED', 0, write_mode, error_message=str(e))
        raise


def _build_bq_client(bq_project, secret_alias):
    secret_json = _snowflake.get_generic_secret_string(secret_alias)
    info = json.loads(secret_json)
    credentials = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
    )
    return bigquery.Client(project=bq_project, credentials=credentials)


def _build_query(bq_project, bq_query, bq_dataset, bq_table, watermark_column, window_start, window_end):
    if bq_query:
        if "{load_window_start}" in bq_query or "{load_window_end}" in bq_query:
            return bq_query.format(load_window_start=window_start, load_window_end=window_end)
        return bq_query

    query = f"SELECT * FROM `{bq_project}.{bq_dataset}.{bq_table}`"
    if watermark_column and (window_start or window_end):
        conditions = []
        if window_start:
            conditions.append(f"`{watermark_column}` >= '{window_start}'")
        if window_end:
            conditions.append(f"`{watermark_column}` < '{window_end}'")
        query += " WHERE " + " AND ".join(conditions)
    return query


def _merge_staging_into_target(session, database, schema, staging_table, target_table, merge_keys):
    stg_fqn = f'"{database}"."{schema}"."{staging_table}"'
    tgt_fqn = f'"{database}"."{schema}"."{target_table}"'

    session.sql(f'CREATE TABLE IF NOT EXISTS {tgt_fqn} LIKE {stg_fqn}').collect()

    columns = session.table(stg_fqn).columns  # already-quoted identifiers
    key_cols = {k.strip().strip('"').upper() for k in merge_keys.split(",")}

    on_clause = " AND ".join(
        f"t.{c} = s.{c}" for c in columns if c.strip('"').upper() in key_cols
    )
    update_clause = ", ".join(
        f"t.{c} = s.{c}" for c in columns if c.strip('"').upper() not in key_cols
    )
    insert_cols = ", ".join(columns)
    insert_values = ", ".join(f"s.{c}" for c in columns)

    merge_sql = f"""
        MERGE INTO {tgt_fqn} AS t
        USING {stg_fqn} AS s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
    """
    session.sql(merge_sql).collect()


def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _start_audit_run(session, job_name, environment, triggered_by, window_start, window_end):
    if not job_name:
        return None
    try:
        result = session.call(
            f"{AUDIT_SCHEMA}.sp_start_job_run",
            job_name, TOOL_NAME, None, environment, triggered_by,
            _parse_ts(window_start), _parse_ts(window_end),
        )
        return int(result)
    except Exception:
        # Audit logging is best-effort — an unregistered job or missing
        # ELT_AUDIT schema must not stop the actual BigQuery pull.
        return None


def _end_audit_run(session, run_id, status, rows_extracted, write_mode, error_message=None):
    if run_id is None:
        return
    try:
        rows_inserted = rows_extracted if write_mode != 'MERGE' else 0
        rows_updated = rows_extracted if write_mode == 'MERGE' else 0
        session.call(
            f"{AUDIT_SCHEMA}.sp_end_job_run",
            run_id, status, rows_extracted, rows_inserted, rows_updated, 0, 0,
            error_message, None, f"BigQuery connector — write_mode={write_mode}",
        )
    except Exception:
        pass


def _log_audit_error(session, run_id, job_name, message):
    if run_id is None:
        return
    try:
        session.call(f"{AUDIT_SCHEMA}.sp_log_error", run_id, job_name, message, None, None, 'ERROR', None)
    except Exception:
        pass
$$
COMMENT = 'Reusable BigQuery → Snowflake connector. Pulls a BigQuery query or table into a Snowflake table via APPEND, OVERWRITE, or MERGE.';


-- ── GRANTS (adjust role name) ─────────────────────────────────────────────
-- GRANT USAGE ON PROCEDURE sp_bq_to_snowflake(
--     VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
--     VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, NUMBER, VARCHAR, VARCHAR, VARCHAR
-- ) TO ROLE my_pipeline_role;


/* ─────────────────────────────────────────────────────────────────────────────
   EXAMPLE USAGE
   ───────────────────────────────────────────────────────────────────────────*/

-- Optional but recommended: register the job so runs show up in
-- VW_JOB_SUMMARY / VW_FAILED_RUNS (see SQL/ETLAuditLog_EntityRef/ELT_Log.sql).
CALL ANALYTICS_DB.ELT_AUDIT.sp_register_job(
    'LOAD_GA_SESSIONS_FROM_BQ',
    'Pulls daily GA4 export sessions table from BigQuery',
    'BigQuery',
    'RAW_DB',
    'GA_SESSIONS',
    'INCREMENTAL',
    'Data Engineering'
);

-- Full table pull, append-only
CALL sp_bq_to_snowflake(
    p_job_name        => 'LOAD_GA_SESSIONS_FROM_BQ',
    p_bq_project      => 'my-gcp-project',
    p_bq_dataset      => 'analytics_123456',
    p_bq_table        => 'events_20260720',
    p_target_database => 'RAW_DB',
    p_target_schema   => 'BIGQUERY_RAW',
    p_target_table    => 'GA_SESSIONS',
    p_write_mode      => 'APPEND'
);

-- Full refresh (truncate + reload) with a custom BigQuery SQL query
CALL sp_bq_to_snowflake(
    p_job_name        => 'LOAD_GA_SESSIONS_FROM_BQ',
    p_bq_project      => 'my-gcp-project',
    p_bq_query        => 'SELECT user_pseudo_id, event_name, event_timestamp FROM `my-gcp-project.analytics_123456.events_*` WHERE _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", CURRENT_DATE())',
    p_target_database => 'RAW_DB',
    p_target_schema   => 'BIGQUERY_RAW',
    p_target_table    => 'GA_EVENTS_TODAY',
    p_write_mode      => 'OVERWRITE'
);

-- Incremental upsert (MERGE) using a watermark column and a load window
CALL sp_bq_to_snowflake(
    p_job_name          => 'LOAD_ORDERS_FROM_BQ',
    p_bq_project        => 'my-gcp-project',
    p_bq_dataset        => 'ecommerce',
    p_bq_table          => 'orders',
    p_target_database   => 'RAW_DB',
    p_target_schema     => 'BIGQUERY_RAW',
    p_target_table      => 'ORDERS',
    p_write_mode        => 'MERGE',
    p_merge_keys        => 'ORDER_ID',
    p_watermark_column  => 'updated_at',
    p_load_window_start => '2026-07-20T00:00:00',
    p_load_window_end   => '2026-07-21T00:00:00'
);

-- Custom query using the {load_window_start}/{load_window_end} placeholders
CALL sp_bq_to_snowflake(
    p_job_name          => 'LOAD_ORDERS_FROM_BQ',
    p_bq_project        => 'my-gcp-project',
    p_bq_query          => 'SELECT * FROM `my-gcp-project.ecommerce.orders` WHERE updated_at >= "{load_window_start}" AND updated_at < "{load_window_end}"',
    p_target_database   => 'RAW_DB',
    p_target_schema     => 'BIGQUERY_RAW',
    p_target_table      => 'ORDERS',
    p_write_mode        => 'MERGE',
    p_merge_keys        => 'ORDER_ID',
    p_load_window_start => '2026-07-20T00:00:00',
    p_load_window_end   => '2026-07-21T00:00:00'
);

-- Check the result
SELECT * FROM ANALYTICS_DB.ELT_AUDIT.VW_RECENT_RUN_HISTORY
WHERE JOB_NAME = 'LOAD_ORDERS_FROM_BQ'
ORDER BY START_TIME DESC
LIMIT 10;


/* ─────────────────────────────────────────────────────────────────────────────
   SCHEDULING
   Wrap the CALL in a Snowflake Task to run on a schedule instead of an
   external orchestrator:

   CREATE OR REPLACE TASK BQ_CONNECTOR.task_load_orders_from_bq
     WAREHOUSE = my_wh
     SCHEDULE = 'USING CRON 0 6 * * * UTC'
   AS
     CALL BQ_CONNECTOR.sp_bq_to_snowflake(
       p_job_name => 'LOAD_ORDERS_FROM_BQ', p_bq_project => 'my-gcp-project',
       p_bq_dataset => 'ecommerce', p_bq_table => 'orders',
       p_target_database => 'RAW_DB', p_target_schema => 'BIGQUERY_RAW',
       p_target_table => 'ORDERS', p_write_mode => 'MERGE', p_merge_keys => 'ORDER_ID'
     );

   ALTER TASK BQ_CONNECTOR.task_load_orders_from_bq RESUME;
   ───────────────────────────────────────────────────────────────────────────*/


/* ─────────────────────────────────────────────────────────────────────────────
   MULTIPLE BQ CONNECTIONS
   The default deployment binds one secret alias, 'bq_service_account', to
   one GCP service account. To pull from a second GCP project/service
   account without redeploying the procedure signature:

   1. Create a second secret in 00_setup_external_access.sql, e.g.
      bq_connector_secret_projectb, with that project's service account key.
   2. Add it to the integration's ALLOWED_AUTHENTICATION_SECRETS list.
   3. Bind it as a second alias on this procedure:
        ALTER PROCEDURE sp_bq_to_snowflake(...)
          SET SECRETS = (
            'bq_service_account'   = bq_connector_secret,
            'bq_service_account_b' = bq_connector_secret_projectb
          );
   4. Pass p_secret_alias => 'bq_service_account_b' on calls for that project.
   ───────────────────────────────────────────────────────────────────────────*/
