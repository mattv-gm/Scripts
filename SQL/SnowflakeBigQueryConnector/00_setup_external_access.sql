/*
================================================================================
  BIGQUERY CONNECTOR — ONE-TIME SETUP
  Network rule + secret + external access integration that let a Python
  stored procedure running INSIDE Snowflake call out to Google BigQuery.

  WHY THIS EXISTS
  ────────────────
  Snowflake sandboxes Python stored procedures by default — no outbound
  network access, no filesystem, no way to leak credentials into query
  history. To call BigQuery's API from inside Snowflake you must explicitly
  grant a narrow slice of network access via an EXTERNAL ACCESS INTEGRATION,
  and store the GCP service-account key in a SECRET object rather than
  pasting it into procedure code (which would land in query/DDL history).

  Run this once per Snowflake account (or once per environment). The
  connector procedure in bq_to_snowflake_connector.sql depends on the
  objects created here.

  SETUP INSTRUCTIONS
  ───────────────────
  1. Requires a role with CREATE INTEGRATION privilege (e.g. ACCOUNTADMIN)
     for section 3. Sections 1-2 and 4 can run under a role with CREATE
     NETWORK RULE / CREATE SECRET / USAGE-grant privileges on the schema.
  2. Replace ANALYTICS_DB below with your actual database name.
  3. In section 2, paste the GCP service account JSON key (see "CREATING
     THE GCP SERVICE ACCOUNT" below). Rotate later with ALTER SECRET.
  4. Update the role name in section 4 grants to whatever role runs your
     pipelines.

  CREATING THE GCP SERVICE ACCOUNT (one-time, in GCP)
  ─────────────────────────────────────────────────────
  1. In the source GCP project: IAM & Admin > Service Accounts > Create.
  2. Grant it "BigQuery Data Viewer" (read table data) and "BigQuery Job
     User" (run query jobs) — least privilege needed to extract data.
     Do NOT grant broader BigQuery Admin roles.
  3. Create a JSON key for the service account and download it.
  4. Paste the full JSON contents (as a single-line string) into the
     SECRET_STRING in section 2 below.

  SETUP ORDER
  ───────────
  Run sections in order: 1 → 2 → 3 → 4
================================================================================
*/

USE DATABASE ANALYTICS_DB;

CREATE SCHEMA IF NOT EXISTS BQ_CONNECTOR
  COMMENT = 'BigQuery-to-Snowflake connector: network access, secrets, and the sp_bq_to_snowflake procedure.';

USE SCHEMA BQ_CONNECTOR;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 1 — NETWORK RULE
   Whitelists exactly the Google hosts the connector needs: the BigQuery
   REST API and the OAuth token endpoints used to exchange the service
   account key for an access token. Nothing else is reachable from the
   procedure.
   ───────────────────────────────────────────────────────────────────────────*/

CREATE OR REPLACE NETWORK RULE bq_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'bigquery.googleapis.com',
    'oauth2.googleapis.com',
    'www.googleapis.com'
  )
  COMMENT = 'Outbound hosts required to authenticate to and query Google BigQuery.';


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 2 — SECRET
   Holds the GCP service account JSON key. Stored encrypted by Snowflake,
   never appears in query text/history, and is only readable from inside a
   procedure that has been explicitly granted it via the SECRETS clause.

   To support pulling from more than one GCP project/service account, create
   additional secrets here (e.g. bq_connector_secret_projectb) and bind them
   as extra aliases when the connector procedure is created/altered — see
   the "MULTIPLE BQ CONNECTIONS" note at the bottom of bq_to_snowflake_connector.sql.
   ───────────────────────────────────────────────────────────────────────────*/

CREATE OR REPLACE SECRET bq_connector_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = '<paste full GCP service account JSON key here>'
  COMMENT = 'GCP service account JSON key for the BigQuery connector. Rotate with ALTER SECRET bq_connector_secret SET SECRET_STRING = ...';


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 3 — EXTERNAL ACCESS INTEGRATION
   The object that actually grants a procedure permission to use the network
   rule + secret above. Requires ACCOUNTADMIN (or a role with CREATE
   INTEGRATION) to create.
   ───────────────────────────────────────────────────────────────────────────*/

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION bq_external_access_integration
  ALLOWED_NETWORK_RULES = (bq_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (bq_connector_secret)
  ENABLED = TRUE
  COMMENT = 'Lets the BigQuery connector procedure call Google BigQuery over HTTPS.';


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 4 — GRANTS
   Adjust role names to your environment. The role that will CALL the
   connector procedure needs USAGE on the integration and READ on the
   secret in addition to EXECUTE on the procedure itself (granted in
   bq_to_snowflake_connector.sql).
   ───────────────────────────────────────────────────────────────────────────*/

-- GRANT USAGE ON INTEGRATION bq_external_access_integration TO ROLE my_pipeline_role;
-- GRANT READ ON SECRET bq_connector_secret TO ROLE my_pipeline_role;
-- GRANT USAGE ON SCHEMA BQ_CONNECTOR TO ROLE my_pipeline_role;
