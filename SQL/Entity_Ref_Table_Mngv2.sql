-- ============================================================
-- CORRECTED: sp_upsert_entity — Snowflake-compatible syntax
-- Change summary:
--   - Added EXCEPTION declaration to DECLARE block
--   - Added v_error_msg VARCHAR to DECLARE block
--   - Replaced all RAISE EXCEPTION 'string' with:
--       v_error_msg := '...'; RAISE validation_error;
--   - Added EXCEPTION handler block to surface dynamic message
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
    -- ── Exceptions ────────────────────────────────────────────
    -- Snowflake requires exceptions to be declared here.
    -- We use a single validation_error for all input checks
    -- and surface the specific message via v_error_msg.
    validation_error    EXCEPTION(-20001, 'Validation error — see raised message for details');

    -- ── Variables ─────────────────────────────────────────────
    v_error_msg         VARCHAR;        -- holds the specific error text before RAISE
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
        v_error_msg := 'sp_upsert_entity: p_entity_id is required and cannot be empty.';
        RAISE validation_error;
    END IF;

    IF (p_entity_name IS NULL OR TRIM(p_entity_name) = '') THEN
        v_error_msg := 'sp_upsert_entity: p_entity_name is required and cannot be empty.';
        RAISE validation_error;
    END IF;

    IF (p_entity_short_code IS NULL OR TRIM(p_entity_short_code) = '') THEN
        v_error_msg := 'sp_upsert_entity: p_entity_short_code is required and cannot be empty.';
        RAISE validation_error;
    END IF;

    IF (p_updated_by IS NULL OR TRIM(p_updated_by) = '') THEN
        v_error_msg := 'sp_upsert_entity: p_updated_by is required and cannot be empty.';
        RAISE validation_error;
    END IF;

    IF (p_effective_from IS NULL) THEN
        v_error_msg := 'sp_upsert_entity: p_effective_from is required.';
        RAISE validation_error;
    END IF;

    -- ── entity_type validation ────────────────────────────────

    IF (UPPER(p_entity_type) NOT IN ('HOLDING', 'SUBSIDIARY')) THEN
        v_error_msg := 'sp_upsert_entity: p_entity_type must be HOLDING or SUBSIDIARY. Received: ' || COALESCE(p_entity_type, 'NULL');
        RAISE validation_error;
    END IF;

    IF (UPPER(p_entity_type) = 'HOLDING' AND p_parent_entity_id IS NOT NULL) THEN
        v_error_msg := 'sp_upsert_entity: A HOLDING entity must have p_parent_entity_id = NULL. Received: ' || p_parent_entity_id;
        RAISE validation_error;
    END IF;

    IF (UPPER(p_entity_type) = 'SUBSIDIARY' AND (p_parent_entity_id IS NULL OR TRIM(p_parent_entity_id) = '')) THEN
        v_error_msg := 'sp_upsert_entity: A SUBSIDIARY entity requires a valid p_parent_entity_id.';
        RAISE validation_error;
    END IF;

    -- ── parent_entity_id existence + active check ─────────────

    IF (p_parent_entity_id IS NOT NULL) THEN

        SELECT COUNT(*)
        INTO   v_parent_count
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = p_parent_entity_id;

        IF (v_parent_count = 0) THEN
            v_error_msg := 'sp_upsert_entity: p_parent_entity_id "' || p_parent_entity_id || '" does not exist in ref.entity.';
            RAISE validation_error;
        END IF;

        SELECT is_active
        INTO   v_parent_active
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = p_parent_entity_id;

        IF (NOT v_parent_active) THEN
            v_error_msg := 'sp_upsert_entity: p_parent_entity_id "' || p_parent_entity_id || '" exists but is inactive. Cannot set an inactive entity as parent.';
            RAISE validation_error;
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

    SELECT MAX(change_id)
    INTO   v_change_id
    FROM   <DATABASE>.ref.change_log
    WHERE  table_name        = 'ref.entity'
    AND    record_id         = p_entity_id
    AND    change_timestamp  = v_now;

    -- ── Return full row summary ───────────────────────────────

    RETURN TABLE (
        SELECT
            v_action        AS action,
            v_change_id     AS change_id,
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

EXCEPTION
    -- Re-raise validation_error with the specific dynamic message
    -- so the caller sees exactly which check failed.
    WHEN validation_error THEN
        RAISE EXCEPTION v_error_msg;
    WHEN OTHER THEN
        RAISE;

END;
$$;


-- ============================================================
-- CORRECTED: sp_upsert_source_system — Snowflake-compatible
-- Same pattern: declared exception + v_error_msg variable
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
    -- ── Exceptions ────────────────────────────────────────────
    validation_error    EXCEPTION(-20001, 'Validation error — see raised message for details');

    -- ── Variables ─────────────────────────────────────────────
    v_error_msg         VARCHAR;
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
        v_error_msg := 'sp_upsert_source_system: p_source_system_id is required and cannot be empty.';
        RAISE validation_error;
    END IF;

    IF (p_source_system_name IS NULL OR TRIM(p_source_system_name) = '') THEN
        v_error_msg := 'sp_upsert_source_system: p_source_system_name is required and cannot be empty.';
        RAISE validation_error;
    END IF;

    IF (p_updated_by IS NULL OR TRIM(p_updated_by) = '') THEN
        v_error_msg := 'sp_upsert_source_system: p_updated_by is required and cannot be empty.';
        RAISE validation_error;
    END IF;

    IF (p_onboarded_date IS NULL) THEN
        v_error_msg := 'sp_upsert_source_system: p_onboarded_date is required.';
        RAISE validation_error;
    END IF;

    -- ── source_system_id format check (lowercase snake_case) ──

    IF (p_source_system_id != LOWER(p_source_system_id)) THEN
        v_error_msg := 'sp_upsert_source_system: p_source_system_id must be lowercase. Received: "' || p_source_system_id || '". Use snake_case (e.g. oracle_erp, s3_file_drop).';
        RAISE validation_error;
    END IF;

    IF (p_source_system_id LIKE '% %') THEN
        v_error_msg := 'sp_upsert_source_system: p_source_system_id must not contain spaces. Use underscores (e.g. oracle_erp).';
        RAISE validation_error;
    END IF;

    -- ── category validation ───────────────────────────────────

    IF (UPPER(p_source_system_category) NOT IN ('CRM', 'ERP', 'FILE', 'API', 'DATABASE', 'INTERNAL')) THEN
        v_error_msg := 'sp_upsert_source_system: p_source_system_category must be one of CRM, ERP, FILE, API, DATABASE, INTERNAL. Received: "' || COALESCE(p_source_system_category, 'NULL') || '".';
        RAISE validation_error;
    END IF;

    -- ── connection_type validation (when provided) ────────────

    IF (p_connection_type IS NOT NULL
        AND UPPER(p_connection_type) NOT IN ('JDBC', 'REST_API', 'S3', 'SFTP', 'SNOWPIPE', 'INTERNAL')) THEN
        v_error_msg := 'sp_upsert_source_system: p_connection_type must be one of JDBC, REST_API, S3, SFTP, SNOWPIPE, INTERNAL. Received: "' || p_connection_type || '".';
        RAISE validation_error;
    END IF;

    -- ── environment validation (when provided) ────────────────

    IF (p_environment IS NOT NULL
        AND UPPER(p_environment) NOT IN ('PROD', 'UAT', 'DEV')) THEN
        v_error_msg := 'sp_upsert_source_system: p_environment must be PROD, UAT, or DEV. Received: "' || p_environment || '".';
        RAISE validation_error;
    END IF;

    -- ── owning_entity_id existence + active check ─────────────

    IF (p_owning_entity_id IS NOT NULL) THEN

        SELECT COUNT(*)
        INTO   v_entity_count
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = p_owning_entity_id;

        IF (v_entity_count = 0) THEN
            v_error_msg := 'sp_upsert_source_system: p_owning_entity_id "' || p_owning_entity_id || '" does not exist in ref.entity.';
            RAISE validation_error;
        END IF;

        SELECT is_active
        INTO   v_entity_active
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = p_owning_entity_id;

        IF (NOT v_entity_active) THEN
            v_error_msg := 'sp_upsert_source_system: p_owning_entity_id "' || p_owning_entity_id || '" exists but is inactive. Cannot assign an inactive entity as owner.';
            RAISE validation_error;
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

    SELECT MAX(change_id)
    INTO   v_change_id
    FROM   <DATABASE>.ref.change_log
    WHERE  table_name        = 'ref.source_system'
    AND    record_id         = p_source_system_id
    AND    change_timestamp  = v_now;

    -- ── Return full row summary ───────────────────────────────

    RETURN TABLE (
        SELECT
            v_action        AS action,
            v_change_id     AS change_id,
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

EXCEPTION
    WHEN validation_error THEN
        RAISE EXCEPTION v_error_msg;
    WHEN OTHER THEN
        RAISE;

END;
$$;
