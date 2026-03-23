-- ============================================================
-- STORED PROCEDURES: Reference Table Management
-- Schema:    <DATABASE>.ref
-- Updated:   March 2026
--
-- Procedures:
--   1. sp_upsert_entity        — insert or update ref.entity
--   2. sp_upsert_source_system — insert or update ref.source_system
--
-- Both procedures:
--   - Upsert: INSERT on new ID, UPDATE on existing ID
--   - Validate all referenced foreign IDs before writing
--   - Raise a descriptive error on any validation failure
--   - Return a full summary result set of the affected row
--   - Log every insert and update to ref.change_log
--   - Are idempotent: calling twice with the same args is safe
--
-- Prerequisites:
--   Run Section 0 first to create ref.change_log if it does
--   not already exist.
-- ============================================================


-- ============================================================
-- SECTION 0: ref.change_log (audit trail for both procedures)
-- ============================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.ref.change_log (
    change_id           NUMBER          AUTOINCREMENT PRIMARY KEY,
    change_timestamp    TIMESTAMP_NTZ   NOT NULL  DEFAULT SYSDATE(),
    table_name          VARCHAR(100)    NOT NULL,   -- ref.entity or ref.source_system
    record_id           VARCHAR(100)    NOT NULL,   -- entity_id or source_system_id
    action              VARCHAR(10)     NOT NULL,   -- INSERT or UPDATE
    changed_by          VARCHAR(100)    NOT NULL,
    change_summary      VARCHAR(2000)               -- human-readable description of what changed
);


-- ============================================================
-- SECTION 1: sp_upsert_entity
-- ============================================================
--
-- Parameters:
--   p_entity_id           VARCHAR  Required. Format ENT-NNN.
--   p_entity_name         VARCHAR  Required. Full legal name.
--   p_entity_short_code   VARCHAR  Required. Abbreviation.
--   p_entity_type         VARCHAR  Required. HOLDING or SUBSIDIARY.
--   p_parent_entity_id    VARCHAR  Required for SUBSIDIARY; NULL for HOLDING.
--   p_country_code        VARCHAR  Optional. ISO 3166-1 alpha-2.
--   p_timezone            VARCHAR  Optional. IANA timezone string.
--   p_is_active           BOOLEAN  Required. TRUE or FALSE.
--   p_effective_from      DATE     Required.
--   p_effective_to        DATE     Optional. NULL = currently active.
--   p_notes               VARCHAR  Optional. Free text.
--   p_updated_by          VARCHAR  Required. Name or service account.
--
-- Returns: single result set with all columns of the affected row
--          plus action (INSERT or UPDATE) and change_id.
--
-- Raises errors when:
--   - p_entity_type is not HOLDING or SUBSIDIARY
--   - p_entity_type is SUBSIDIARY and p_parent_entity_id is NULL
--   - p_entity_type is HOLDING and p_parent_entity_id is not NULL
--   - p_parent_entity_id is provided but does not exist in ref.entity
--   - p_parent_entity_id references an inactive entity
--   - p_updated_by is NULL or empty
-- ============================================================

CREATE OR REPLACE PROCEDURE <DATABASE>.ref.sp_upsert_entity(
    p_entity_id         VARCHAR,
    p_entity_name       VARCHAR,
    p_entity_short_code VARCHAR,
    p_entity_type       VARCHAR,
    p_parent_entity_id  VARCHAR,
    p_country_code      VARCHAR,
    p_timezone          VARCHAR,
    p_is_active         BOOLEAN,
    p_effective_from    DATE,
    p_effective_to      DATE,
    p_notes             VARCHAR,
    p_updated_by        VARCHAR
)
RETURNS TABLE (
    action              VARCHAR,
    change_id           NUMBER,
    entity_id           VARCHAR,
    entity_name         VARCHAR,
    entity_short_code   VARCHAR,
    entity_type         VARCHAR,
    parent_entity_id    VARCHAR,
    country_code        VARCHAR,
    timezone            VARCHAR,
    is_active           BOOLEAN,
    effective_from      DATE,
    effective_to        DATE,
    notes               VARCHAR,
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    updated_by          VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    v_action            VARCHAR;
    v_existing_count    INTEGER;
    v_parent_count      INTEGER;
    v_parent_active     BOOLEAN;
    v_change_id         NUMBER;
    v_change_summary    VARCHAR;
    v_now               TIMESTAMP_NTZ := SYSDATE();
BEGIN

    -- ── Input validation ──────────────────────────────────────

    IF (p_entity_id IS NULL OR TRIM(p_entity_id) = '') THEN
        RAISE EXCEPTION 'sp_upsert_entity: p_entity_id is required and cannot be empty.';
    END IF;

    IF (p_entity_name IS NULL OR TRIM(p_entity_name) = '') THEN
        RAISE EXCEPTION 'sp_upsert_entity: p_entity_name is required and cannot be empty.';
    END IF;

    IF (p_entity_short_code IS NULL OR TRIM(p_entity_short_code) = '') THEN
        RAISE EXCEPTION 'sp_upsert_entity: p_entity_short_code is required and cannot be empty.';
    END IF;

    IF (p_updated_by IS NULL OR TRIM(p_updated_by) = '') THEN
        RAISE EXCEPTION 'sp_upsert_entity: p_updated_by is required and cannot be empty.';
    END IF;

    IF (p_effective_from IS NULL) THEN
        RAISE EXCEPTION 'sp_upsert_entity: p_effective_from is required.';
    END IF;

    -- ── entity_type validation ────────────────────────────────

    IF (UPPER(p_entity_type) NOT IN ('HOLDING', 'SUBSIDIARY')) THEN
        RAISE EXCEPTION 'sp_upsert_entity: p_entity_type must be HOLDING or SUBSIDIARY. Received: %', p_entity_type;
    END IF;

    IF (UPPER(p_entity_type) = 'HOLDING' AND p_parent_entity_id IS NOT NULL) THEN
        RAISE EXCEPTION 'sp_upsert_entity: A HOLDING entity must have p_parent_entity_id = NULL. Received: %', p_parent_entity_id;
    END IF;

    IF (UPPER(p_entity_type) = 'SUBSIDIARY' AND (p_parent_entity_id IS NULL OR TRIM(p_parent_entity_id) = '')) THEN
        RAISE EXCEPTION 'sp_upsert_entity: A SUBSIDIARY entity requires a valid p_parent_entity_id.';
    END IF;

    -- ── parent_entity_id existence + active check ─────────────

    IF (p_parent_entity_id IS NOT NULL) THEN

        SELECT COUNT(*)
        INTO   v_parent_count
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = p_parent_entity_id;

        IF (v_parent_count = 0) THEN
            RAISE EXCEPTION 'sp_upsert_entity: p_parent_entity_id "%" does not exist in ref.entity.', p_parent_entity_id;
        END IF;

        SELECT is_active
        INTO   v_parent_active
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = p_parent_entity_id;

        IF (NOT v_parent_active) THEN
            RAISE EXCEPTION 'sp_upsert_entity: p_parent_entity_id "%" exists but is inactive. Cannot set an inactive entity as parent.', p_parent_entity_id;
        END IF;

    END IF;

    -- ── Determine INSERT vs UPDATE ────────────────────────────

    SELECT COUNT(*)
    INTO   v_existing_count
    FROM   <DATABASE>.ref.entity
    WHERE  entity_id = p_entity_id;

    IF (v_existing_count = 0) THEN
        v_action := 'INSERT';
    ELSE
        v_action := 'UPDATE';
    END IF;

    -- ── Upsert ────────────────────────────────────────────────

    IF (v_action = 'INSERT') THEN

        INSERT INTO <DATABASE>.ref.entity (
            entity_id, entity_name, entity_short_code, entity_type,
            parent_entity_id, country_code, timezone,
            is_active, effective_from, effective_to, notes,
            created_at, updated_at, updated_by
        )
        VALUES (
            p_entity_id, p_entity_name, p_entity_short_code, UPPER(p_entity_type),
            p_parent_entity_id, p_country_code, p_timezone,
            p_is_active, p_effective_from, p_effective_to, p_notes,
            v_now, v_now, p_updated_by
        );

        v_change_summary := 'INSERT: new entity ' || p_entity_id || ' (' || p_entity_name || ') type=' || UPPER(p_entity_type);

    ELSE

        UPDATE <DATABASE>.ref.entity
        SET
            entity_name         = p_entity_name,
            entity_short_code   = p_entity_short_code,
            entity_type         = UPPER(p_entity_type),
            parent_entity_id    = p_parent_entity_id,
            country_code        = p_country_code,
            timezone            = p_timezone,
            is_active           = p_is_active,
            effective_from      = p_effective_from,
            effective_to        = p_effective_to,
            notes               = p_notes,
            updated_at          = v_now,
            updated_by          = p_updated_by
        WHERE entity_id = p_entity_id;

        v_change_summary := 'UPDATE: entity ' || p_entity_id || ' (' || p_entity_name || ') updated by ' || p_updated_by;

    END IF;

    -- ── Log to change_log ─────────────────────────────────────

    INSERT INTO <DATABASE>.ref.change_log (
        change_timestamp, table_name, record_id, action, changed_by, change_summary
    )
    VALUES (
        v_now, 'ref.entity', p_entity_id, v_action, p_updated_by, v_change_summary
    );

    SELECT MAX(change_id) INTO v_change_id
    FROM   <DATABASE>.ref.change_log
    WHERE  table_name = 'ref.entity'
    AND    record_id  = p_entity_id
    AND    change_timestamp = v_now;

    -- ── Return full row summary ───────────────────────────────

    RETURN TABLE (
        SELECT
            v_action            AS action,
            v_change_id         AS change_id,
            entity_id,
            entity_name,
            entity_short_code,
            entity_type,
            parent_entity_id,
            country_code,
            timezone,
            is_active,
            effective_from,
            effective_to,
            notes,
            created_at,
            updated_at,
            updated_by
        FROM <DATABASE>.ref.entity
        WHERE entity_id = p_entity_id
    );

END;
$$;


-- ============================================================
-- SECTION 2: sp_upsert_source_system
-- ============================================================
--
-- Parameters:
--   p_source_system_id        VARCHAR  Required. Lowercase snake_case.
--   p_source_system_name      VARCHAR  Required. Full display name.
--   p_source_system_category  VARCHAR  Required. CRM/ERP/FILE/API/DATABASE/INTERNAL.
--   p_description             VARCHAR  Optional.
--   p_owning_entity_id        VARCHAR  Optional. FK to ref.entity.
--   p_owner_team              VARCHAR  Optional.
--   p_owner_contact           VARCHAR  Optional.
--   p_connection_type         VARCHAR  Optional. JDBC/REST_API/S3/SFTP/SNOWPIPE/INTERNAL.
--   p_environment             VARCHAR  Optional. PROD/UAT/DEV.
--   p_is_active               BOOLEAN  Required.
--   p_onboarded_date          DATE     Required.
--   p_decommissioned_date     DATE     Optional.
--   p_notes                   VARCHAR  Optional.
--   p_updated_by              VARCHAR  Required.
--
-- Returns: single result set with all columns of the affected row
--          plus action (INSERT or UPDATE) and change_id.
--
-- Raises errors when:
--   - p_source_system_id contains uppercase or spaces
--   - p_source_system_category is not in the approved list
--   - p_connection_type is provided but not in the approved list
--   - p_environment is provided but not PROD, UAT, or DEV
--   - p_owning_entity_id is provided but does not exist in ref.entity
--   - p_owning_entity_id references an inactive entity
--   - p_updated_by is NULL or empty
-- ============================================================

CREATE OR REPLACE PROCEDURE <DATABASE>.ref.sp_upsert_source_system(
    p_source_system_id          VARCHAR,
    p_source_system_name        VARCHAR,
    p_source_system_category    VARCHAR,
    p_description               VARCHAR,
    p_owning_entity_id          VARCHAR,
    p_owner_team                VARCHAR,
    p_owner_contact             VARCHAR,
    p_connection_type           VARCHAR,
    p_environment               VARCHAR,
    p_is_active                 BOOLEAN,
    p_onboarded_date            DATE,
    p_decommissioned_date       DATE,
    p_notes                     VARCHAR,
    p_updated_by                VARCHAR
)
RETURNS TABLE (
    action                  VARCHAR,
    change_id               NUMBER,
    source_system_id        VARCHAR,
    source_system_name      VARCHAR,
    source_system_category  VARCHAR,
    description             VARCHAR,
    owning_entity_id        VARCHAR,
    owner_team              VARCHAR,
    owner_contact           VARCHAR,
    connection_type         VARCHAR,
    environment             VARCHAR,
    is_active               BOOLEAN,
    onboarded_date          DATE,
    decommissioned_date     DATE,
    notes                   VARCHAR,
    created_at              TIMESTAMP_NTZ,
    updated_at              TIMESTAMP_NTZ,
    updated_by              VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    v_action            VARCHAR;
    v_existing_count    INTEGER;
    v_entity_count      INTEGER;
    v_entity_active     BOOLEAN;
    v_change_id         NUMBER;
    v_change_summary    VARCHAR;
    v_now               TIMESTAMP_NTZ := SYSDATE();
BEGIN

    -- ── Input validation ──────────────────────────────────────

    IF (p_source_system_id IS NULL OR TRIM(p_source_system_id) = '') THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_source_system_id is required and cannot be empty.';
    END IF;

    IF (p_source_system_name IS NULL OR TRIM(p_source_system_name) = '') THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_source_system_name is required and cannot be empty.';
    END IF;

    IF (p_updated_by IS NULL OR TRIM(p_updated_by) = '') THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_updated_by is required and cannot be empty.';
    END IF;

    IF (p_onboarded_date IS NULL) THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_onboarded_date is required.';
    END IF;

    -- ── source_system_id format check (lowercase snake_case) ──
    -- Reject if it contains uppercase letters or spaces

    IF (p_source_system_id != LOWER(p_source_system_id)) THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_source_system_id must be lowercase. Received: "%". Use snake_case (e.g. oracle_erp, s3_file_drop).', p_source_system_id;
    END IF;

    IF (p_source_system_id LIKE '% %') THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_source_system_id must not contain spaces. Use underscores (e.g. oracle_erp).';
    END IF;

    -- ── category validation ───────────────────────────────────

    IF (UPPER(p_source_system_category) NOT IN ('CRM', 'ERP', 'FILE', 'API', 'DATABASE', 'INTERNAL')) THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_source_system_category must be one of CRM, ERP, FILE, API, DATABASE, INTERNAL. Received: "%".', p_source_system_category;
    END IF;

    -- ── connection_type validation (when provided) ────────────

    IF (p_connection_type IS NOT NULL
        AND UPPER(p_connection_type) NOT IN ('JDBC', 'REST_API', 'S3', 'SFTP', 'SNOWPIPE', 'INTERNAL')) THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_connection_type must be one of JDBC, REST_API, S3, SFTP, SNOWPIPE, INTERNAL. Received: "%".', p_connection_type;
    END IF;

    -- ── environment validation (when provided) ────────────────

    IF (p_environment IS NOT NULL
        AND UPPER(p_environment) NOT IN ('PROD', 'UAT', 'DEV')) THEN
        RAISE EXCEPTION 'sp_upsert_source_system: p_environment must be PROD, UAT, or DEV. Received: "%".', p_environment;
    END IF;

    -- ── owning_entity_id existence + active check ─────────────

    IF (p_owning_entity_id IS NOT NULL) THEN

        SELECT COUNT(*)
        INTO   v_entity_count
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = p_owning_entity_id;

        IF (v_entity_count = 0) THEN
            RAISE EXCEPTION 'sp_upsert_source_system: p_owning_entity_id "%" does not exist in ref.entity.', p_owning_entity_id;
        END IF;

        SELECT is_active
        INTO   v_entity_active
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = p_owning_entity_id;

        IF (NOT v_entity_active) THEN
            RAISE EXCEPTION 'sp_upsert_source_system: p_owning_entity_id "%" exists but is inactive. Cannot assign an inactive entity as owner.', p_owning_entity_id;
        END IF;

    END IF;

    -- ── Determine INSERT vs UPDATE ────────────────────────────

    SELECT COUNT(*)
    INTO   v_existing_count
    FROM   <DATABASE>.ref.source_system
    WHERE  source_system_id = p_source_system_id;

    IF (v_existing_count = 0) THEN
        v_action := 'INSERT';
    ELSE
        v_action := 'UPDATE';
    END IF;

    -- ── Upsert ────────────────────────────────────────────────

    IF (v_action = 'INSERT') THEN

        INSERT INTO <DATABASE>.ref.source_system (
            source_system_id, source_system_name, source_system_category,
            description, owning_entity_id, owner_team, owner_contact,
            connection_type, environment, is_active,
            onboarded_date, decommissioned_date, notes,
            created_at, updated_at, updated_by
        )
        VALUES (
            p_source_system_id, p_source_system_name, UPPER(p_source_system_category),
            p_description, p_owning_entity_id, p_owner_team, p_owner_contact,
            UPPER(p_connection_type), UPPER(p_environment), p_is_active,
            p_onboarded_date, p_decommissioned_date, p_notes,
            v_now, v_now, p_updated_by
        );

        v_change_summary := 'INSERT: new source system ' || p_source_system_id
                         || ' (' || p_source_system_name || ') category=' || UPPER(p_source_system_category);

    ELSE

        UPDATE <DATABASE>.ref.source_system
        SET
            source_system_name      = p_source_system_name,
            source_system_category  = UPPER(p_source_system_category),
            description             = p_description,
            owning_entity_id        = p_owning_entity_id,
            owner_team              = p_owner_team,
            owner_contact           = p_owner_contact,
            connection_type         = UPPER(p_connection_type),
            environment             = UPPER(p_environment),
            is_active               = p_is_active,
            onboarded_date          = p_onboarded_date,
            decommissioned_date     = p_decommissioned_date,
            notes                   = p_notes,
            updated_at              = v_now,
            updated_by              = p_updated_by
        WHERE source_system_id = p_source_system_id;

        v_change_summary := 'UPDATE: source system ' || p_source_system_id
                         || ' (' || p_source_system_name || ') updated by ' || p_updated_by;

    END IF;

    -- ── Log to change_log ─────────────────────────────────────

    INSERT INTO <DATABASE>.ref.change_log (
        change_timestamp, table_name, record_id, action, changed_by, change_summary
    )
    VALUES (
        v_now, 'ref.source_system', p_source_system_id, v_action, p_updated_by, v_change_summary
    );

    SELECT MAX(change_id) INTO v_change_id
    FROM   <DATABASE>.ref.change_log
    WHERE  table_name = 'ref.source_system'
    AND    record_id  = p_source_system_id
    AND    change_timestamp = v_now;

    -- ── Return full row summary ───────────────────────────────

    RETURN TABLE (
        SELECT
            v_action            AS action,
            v_change_id         AS change_id,
            source_system_id,
            source_system_name,
            source_system_category,
            description,
            owning_entity_id,
            owner_team,
            owner_contact,
            connection_type,
            environment,
            is_active,
            onboarded_date,
            decommissioned_date,
            notes,
            created_at,
            updated_at,
            updated_by
        FROM <DATABASE>.ref.source_system
        WHERE source_system_id = p_source_system_id
    );

END;
$$;


-- ============================================================
-- SECTION 3: Usage examples
-- ============================================================

-- ── Insert new subsidiary entity ─────────────────────────────
CALL <DATABASE>.ref.sp_upsert_entity(
    'ENT-005',              -- p_entity_id
    'Acme Southwest LLC',   -- p_entity_name
    'ACME_SW',              -- p_entity_short_code
    'SUBSIDIARY',           -- p_entity_type
    'ENT-001',              -- p_parent_entity_id (holding company)
    'US',                   -- p_country_code
    'America/Denver',       -- p_timezone
    TRUE,                   -- p_is_active
    '2026-01-01'::DATE,     -- p_effective_from
    NULL,                   -- p_effective_to (currently active)
    'Acquired January 2026',-- p_notes
    'data_engineering'      -- p_updated_by
);

-- ── Update an existing entity (change name, same entity_id) ──
CALL <DATABASE>.ref.sp_upsert_entity(
    'ENT-002',
    'Acme East LLC',        -- updated name
    'ACME_E',
    'SUBSIDIARY',
    'ENT-001',
    'US',
    'America/New_York',
    TRUE,
    '2020-01-01'::DATE,
    NULL,
    'Name corrected March 2026',
    'data_engineering'
);

-- ── Decommission an entity ────────────────────────────────────
-- (same procedure — just set is_active = FALSE and effective_to)
CALL <DATABASE>.ref.sp_upsert_entity(
    'ENT-004',
    'Acme North LLC',
    'ACME_N',
    'SUBSIDIARY',
    'ENT-001',
    'US',
    'America/Chicago',
    FALSE,                      -- is_active = FALSE
    '2020-01-01'::DATE,
    '2026-03-20'::DATE,         -- effective_to = decommission date
    'Entity dissolved March 2026',
    'data_engineering'
);

-- ── Insert new source system ──────────────────────────────────
CALL <DATABASE>.ref.sp_upsert_source_system(
    'stripe',               -- p_source_system_id (lowercase snake_case)
    'Stripe Payments',      -- p_source_system_name
    'API',                  -- p_source_system_category
    'Payment transactions and subscription data from Stripe', -- p_description
    'ENT-001',              -- p_owning_entity_id
    'Finance Engineering',  -- p_owner_team
    'finance-eng@company.com', -- p_owner_contact
    'REST_API',             -- p_connection_type
    'PROD',                 -- p_environment
    TRUE,                   -- p_is_active
    '2026-03-20'::DATE,     -- p_onboarded_date
    NULL,                   -- p_decommissioned_date
    NULL,                   -- p_notes
    'data_engineering'      -- p_updated_by
);

-- ── Update an existing source system ─────────────────────────
CALL <DATABASE>.ref.sp_upsert_source_system(
    'salesforce',
    'Salesforce CRM',
    'CRM',
    'Customer, opportunity, and case data — includes all subsidiaries',
    'ENT-001',
    'Revenue Operations',   -- updated owner team
    'revops@company.com',
    'REST_API',
    'PROD',
    TRUE,
    '2020-01-01'::DATE,
    NULL,
    NULL,
    'data_engineering'
);

-- ── Retire a source system ────────────────────────────────────
CALL <DATABASE>.ref.sp_upsert_source_system(
    'sftp_inbound',
    'SFTP Inbound (Legacy)',
    'FILE',
    'Legacy SFTP file drop — replaced by S3 direct integration',
    'ENT-001',
    'Data Engineering',
    NULL,
    'SFTP',
    'PROD',
    FALSE,                      -- is_active = FALSE
    '2021-06-01'::DATE,
    '2026-03-20'::DATE,         -- decommissioned_date
    'Migrated to s3_file_drop March 2026',
    'data_engineering'
);


-- ============================================================
-- SECTION 4: Validation queries
-- ============================================================

-- View full change history for a specific entity
SELECT * FROM <DATABASE>.ref.change_log
WHERE  table_name = 'ref.entity'
AND    record_id  = 'ENT-005'
ORDER  BY change_timestamp DESC;

-- View full change history for a specific source system
SELECT * FROM <DATABASE>.ref.change_log
WHERE  table_name = 'ref.source_system'
AND    record_id  = 'stripe'
ORDER  BY change_timestamp DESC;

-- All changes made today
SELECT * FROM <DATABASE>.ref.change_log
WHERE  change_timestamp >= CURRENT_DATE()
ORDER  BY change_timestamp DESC;

-- All changes made by a specific person / service account
SELECT * FROM <DATABASE>.ref.change_log
WHERE  changed_by = 'data_engineering'
ORDER  BY change_timestamp DESC;
