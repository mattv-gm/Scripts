-- ============================================================
-- STORED PROCEDURES: Reference Table Management
-- Schema:    <DATABASE>.ref
-- Updated:   March 2026
-- Version:   1.2 — all exceptions declared in DECLARE block
-- ============================================================


-- ============================================================
-- SECTION 0: ref.change_log
-- ============================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.ref.change_log (
    change_id           NUMBER          AUTOINCREMENT PRIMARY KEY,
    change_timestamp    TIMESTAMP_NTZ   NOT NULL  DEFAULT SYSDATE(),
    table_name          VARCHAR(100)    NOT NULL,
    record_id           VARCHAR(100)    NOT NULL,
    action              VARCHAR(10)     NOT NULL,
    changed_by          VARCHAR(100)    NOT NULL,
    change_summary      VARCHAR(2000)
);


-- ============================================================
-- SECTION 1: sp_upsert_entity
-- ============================================================

CREATE OR REPLACE PROCEDURE <DATABASE>.ref.sp_upsert_entity(
    p_entity_id         VARCHAR,
    p_entity_name       VARCHAR,
    p_entity_short_code VARCHAR,
    p_entity_type       VARCHAR,
    p_parent_entity_id  VARCHAR,
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
    -- ── Declared exceptions ───────────────────────────────────
    -- One declaration per validation check. The message in the
    -- declaration is what the caller sees when the error fires.

    -- Required field checks
    err_entity_id_required      EXCEPTION(-20001, 'sp_upsert_entity: p_entity_id is required and cannot be empty.');
    err_entity_name_required    EXCEPTION(-20002, 'sp_upsert_entity: p_entity_name is required and cannot be empty.');
    err_short_code_required     EXCEPTION(-20003, 'sp_upsert_entity: p_entity_short_code is required and cannot be empty.');
    err_updated_by_required     EXCEPTION(-20004, 'sp_upsert_entity: p_updated_by is required and cannot be empty.');
    err_effective_from_required EXCEPTION(-20005, 'sp_upsert_entity: p_effective_from is required.');

    -- entity_type checks
    err_invalid_entity_type     EXCEPTION(-20006, 'sp_upsert_entity: p_entity_type must be HOLDING or SUBSIDIARY.');
    err_holding_has_parent      EXCEPTION(-20007, 'sp_upsert_entity: A HOLDING entity must have p_parent_entity_id = NULL.');
    err_subsidiary_needs_parent EXCEPTION(-20008, 'sp_upsert_entity: A SUBSIDIARY entity requires a valid p_parent_entity_id.');

    -- parent_entity_id checks
    err_parent_not_found        EXCEPTION(-20009, 'sp_upsert_entity: p_parent_entity_id does not exist in ref.entity.');
    err_parent_inactive         EXCEPTION(-20010, 'sp_upsert_entity: p_parent_entity_id exists but is inactive. Cannot set an inactive entity as parent.');

    -- ── Variables ─────────────────────────────────────────────
    v_result            RESULTSET;
    v_action            VARCHAR;
    v_existing_count    INTEGER;
    v_parent_count      INTEGER;
    v_parent_active     BOOLEAN;
    v_change_id         NUMBER;
    v_change_summary    VARCHAR;
    v_now               TIMESTAMP_NTZ := SYSDATE();

BEGIN

    -- ── Required field validation ─────────────────────────────

    IF (p_entity_id IS NULL OR TRIM(p_entity_id) = '') THEN
        RAISE err_entity_id_required;
    END IF;

    IF (p_entity_name IS NULL OR TRIM(p_entity_name) = '') THEN
        RAISE err_entity_name_required;
    END IF;

    IF (p_entity_short_code IS NULL OR TRIM(p_entity_short_code) = '') THEN
        RAISE err_short_code_required;
    END IF;

    IF (p_updated_by IS NULL OR TRIM(p_updated_by) = '') THEN
        RAISE err_updated_by_required;
    END IF;

    IF (p_effective_from IS NULL) THEN
        RAISE err_effective_from_required;
    END IF;

    -- ── entity_type validation ────────────────────────────────

    IF (UPPER(p_entity_type) NOT IN ('HOLDING', 'SUBSIDIARY')) THEN
        RAISE err_invalid_entity_type;
    END IF;

    IF (UPPER(p_entity_type) = 'HOLDING' AND p_parent_entity_id IS NOT NULL) THEN
        RAISE err_holding_has_parent;
    END IF;

    IF (UPPER(p_entity_type) = 'SUBSIDIARY' AND (p_parent_entity_id IS NULL OR TRIM(p_parent_entity_id) = '')) THEN
        RAISE err_subsidiary_needs_parent;
    END IF;

    -- ── parent_entity_id existence + active check ─────────────

    IF (p_parent_entity_id IS NOT NULL) THEN

        SELECT COUNT(*)
        INTO   v_parent_count
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = :p_parent_entity_id;

        IF (v_parent_count = 0) THEN
            RAISE err_parent_not_found;
        END IF;

        SELECT is_active
        INTO   v_parent_active
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = :p_parent_entity_id;

        IF (NOT v_parent_active) THEN
            RAISE err_parent_inactive;
        END IF;

    END IF;

    -- ── Determine INSERT vs UPDATE ────────────────────────────

    SELECT COUNT(*)
    INTO   v_existing_count
    FROM   <DATABASE>.ref.entity
    WHERE  entity_id = :p_entity_id;

    IF (v_existing_count = 0) THEN
        v_action := 'INSERT';
    ELSE
        v_action := 'UPDATE';
    END IF;

    -- ── Upsert ────────────────────────────────────────────────

    IF (v_action = 'INSERT') THEN

        INSERT INTO <DATABASE>.ref.entity (
            entity_id, entity_name, entity_short_code, entity_type,
            parent_entity_id, is_active, effective_from, effective_to, notes,
            created_at, updated_at, updated_by
        )
        VALUES (
            :p_entity_id, :p_entity_name, :p_entity_short_code, UPPER(:p_entity_type),
            :p_parent_entity_id, :p_is_active, :p_effective_from, :p_effective_to, :p_notes,
            :v_now, :v_now, :p_updated_by
        );

        v_change_summary := 'INSERT: new entity ' || p_entity_id
                         || ' (' || p_entity_name || ') type=' || UPPER(p_entity_type);

    ELSE

        UPDATE <DATABASE>.ref.entity
        SET
            entity_name         = :p_entity_name,
            entity_short_code   = :p_entity_short_code,
            entity_type         = UPPER(:p_entity_type),
            parent_entity_id    = :p_parent_entity_id,
            is_active           = :p_is_active,
            effective_from      = :p_effective_from,
            effective_to        = :p_effective_to,
            notes               = :p_notes,
            updated_at          = :v_now,
            updated_by          = :p_updated_by
        WHERE entity_id = :p_entity_id;

        v_change_summary := 'UPDATE: entity ' || p_entity_id
                         || ' (' || p_entity_name || ') updated by ' || p_updated_by;

    END IF;

    -- ── Log to change_log ─────────────────────────────────────

    INSERT INTO <DATABASE>.ref.change_log (
        change_timestamp, table_name, record_id, action, changed_by, change_summary
    )
    VALUES (
        :v_now, 'ref.entity', :p_entity_id, :v_action, :p_updated_by, :v_change_summary
    );

    SELECT MAX(change_id)
    INTO   v_change_id
    FROM   <DATABASE>.ref.change_log
    WHERE  table_name       = 'ref.entity'
    AND    record_id        = :p_entity_id
    AND    change_timestamp = :v_now;

    -- ── Return full row summary ───────────────────────────────

    v_result := (
        SELECT
            :v_action       AS action,
            :v_change_id    AS change_id,
            entity_id,
            entity_name,
            entity_short_code,
            entity_type,
            parent_entity_id,
            is_active,
            effective_from,
            effective_to,
            notes,
            created_at,
            updated_at,
            updated_by
        FROM <DATABASE>.ref.entity
        WHERE entity_id = :p_entity_id
    );

    RETURN TABLE(v_result);

END;
$$;


-- ============================================================
-- SECTION 2: sp_upsert_source_system
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
    -- ── Declared exceptions ───────────────────────────────────

    -- Required field checks
    err_ss_id_required          EXCEPTION(-20001, 'sp_upsert_source_system: p_source_system_id is required and cannot be empty.');
    err_ss_name_required        EXCEPTION(-20002, 'sp_upsert_source_system: p_source_system_name is required and cannot be empty.');
    err_updated_by_required     EXCEPTION(-20003, 'sp_upsert_source_system: p_updated_by is required and cannot be empty.');
    err_onboarded_date_required EXCEPTION(-20004, 'sp_upsert_source_system: p_onboarded_date is required.');

    -- Format checks
    err_id_not_lowercase        EXCEPTION(-20005, 'sp_upsert_source_system: p_source_system_id must be lowercase snake_case (e.g. oracle_erp). Uppercase characters are not allowed.');
    err_id_has_spaces           EXCEPTION(-20006, 'sp_upsert_source_system: p_source_system_id must not contain spaces. Use underscores (e.g. oracle_erp).');

    -- Controlled vocabulary checks
    err_invalid_category        EXCEPTION(-20007, 'sp_upsert_source_system: p_source_system_category must be one of: CRM, ERP, FILE, API, DATABASE, INTERNAL.');
    err_invalid_connection_type EXCEPTION(-20008, 'sp_upsert_source_system: p_connection_type must be one of: JDBC, REST_API, S3, SFTP, SNOWPIPE, INTERNAL.');
    err_invalid_environment     EXCEPTION(-20009, 'sp_upsert_source_system: p_environment must be one of: PROD, UAT, DEV.');

    -- owning_entity_id checks
    err_entity_not_found        EXCEPTION(-20010, 'sp_upsert_source_system: p_owning_entity_id does not exist in ref.entity.');
    err_entity_inactive         EXCEPTION(-20011, 'sp_upsert_source_system: p_owning_entity_id exists but is inactive. Cannot assign an inactive entity as owner.');

    -- ── Variables ─────────────────────────────────────────────
    v_result            RESULTSET;
    v_action            VARCHAR;
    v_existing_count    INTEGER;
    v_entity_count      INTEGER;
    v_entity_active     BOOLEAN;
    v_change_id         NUMBER;
    v_change_summary    VARCHAR;
    v_now               TIMESTAMP_NTZ := SYSDATE();

BEGIN

    -- ── Required field validation ─────────────────────────────

    IF (p_source_system_id IS NULL OR TRIM(p_source_system_id) = '') THEN
        RAISE err_ss_id_required;
    END IF;

    IF (p_source_system_name IS NULL OR TRIM(p_source_system_name) = '') THEN
        RAISE err_ss_name_required;
    END IF;

    IF (p_updated_by IS NULL OR TRIM(p_updated_by) = '') THEN
        RAISE err_updated_by_required;
    END IF;

    IF (p_onboarded_date IS NULL) THEN
        RAISE err_onboarded_date_required;
    END IF;

    -- ── source_system_id format checks ────────────────────────

    IF (p_source_system_id != LOWER(p_source_system_id)) THEN
        RAISE err_id_not_lowercase;
    END IF;

    IF (p_source_system_id LIKE '% %') THEN
        RAISE err_id_has_spaces;
    END IF;

    -- ── Controlled vocabulary checks ─────────────────────────

    IF (UPPER(p_source_system_category) NOT IN ('CRM', 'ERP', 'FILE', 'API', 'DATABASE', 'INTERNAL')) THEN
        RAISE err_invalid_category;
    END IF;

    IF (p_connection_type IS NOT NULL
        AND UPPER(p_connection_type) NOT IN ('JDBC', 'REST_API', 'S3', 'SFTP', 'SNOWPIPE', 'INTERNAL')) THEN
        RAISE err_invalid_connection_type;
    END IF;

    IF (p_environment IS NOT NULL
        AND UPPER(p_environment) NOT IN ('PROD', 'UAT', 'DEV')) THEN
        RAISE err_invalid_environment;
    END IF;

    -- ── owning_entity_id existence + active check ─────────────

    IF (p_owning_entity_id IS NOT NULL) THEN

        SELECT COUNT(*)
        INTO   v_entity_count
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = :p_owning_entity_id;

        IF (v_entity_count = 0) THEN
            RAISE err_entity_not_found;
        END IF;

        SELECT is_active
        INTO   v_entity_active
        FROM   <DATABASE>.ref.entity
        WHERE  entity_id = :p_owning_entity_id;

        IF (NOT v_entity_active) THEN
            RAISE err_entity_inactive;
        END IF;

    END IF;

    -- ── Determine INSERT vs UPDATE ────────────────────────────

    SELECT COUNT(*)
    INTO   v_existing_count
    FROM   <DATABASE>.ref.source_system
    WHERE  source_system_id = :p_source_system_id;

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
            :p_source_system_id, :p_source_system_name, UPPER(:p_source_system_category),
            :p_description, :p_owning_entity_id, :p_owner_team, :p_owner_contact,
            UPPER(:p_connection_type), UPPER(:p_environment), :p_is_active,
            :p_onboarded_date, :p_decommissioned_date, :p_notes,
            :v_now, :v_now, :p_updated_by
        );

        v_change_summary := 'INSERT: new source system ' || p_source_system_id
                         || ' (' || p_source_system_name || ') category=' || UPPER(p_source_system_category);

    ELSE

        UPDATE <DATABASE>.ref.source_system
        SET
            source_system_name      = :p_source_system_name,
            source_system_category  = UPPER(:p_source_system_category),
            description             = :p_description,
            owning_entity_id        = :p_owning_entity_id,
            owner_team              = :p_owner_team,
            owner_contact           = :p_owner_contact,
            connection_type         = UPPER(:p_connection_type),
            environment             = UPPER(:p_environment),
            is_active               = :p_is_active,
            onboarded_date          = :p_onboarded_date,
            decommissioned_date     = :p_decommissioned_date,
            notes                   = :p_notes,
            updated_at              = :v_now,
            updated_by              = :p_updated_by
        WHERE source_system_id = :p_source_system_id;

        v_change_summary := 'UPDATE: source system ' || p_source_system_id
                         || ' (' || p_source_system_name || ') updated by ' || p_updated_by;

    END IF;

    -- ── Log to change_log ─────────────────────────────────────

    INSERT INTO <DATABASE>.ref.change_log (
        change_timestamp, table_name, record_id, action, changed_by, change_summary
    )
    VALUES (
        :v_now, 'ref.source_system', :p_source_system_id, :v_action, :p_updated_by, :v_change_summary
    );

    SELECT MAX(change_id)
    INTO   v_change_id
    FROM   <DATABASE>.ref.change_log
    WHERE  table_name       = 'ref.source_system'
    AND    record_id        = :p_source_system_id
    AND    change_timestamp = :v_now;

    -- ── Return full row summary ───────────────────────────────

    v_result := (
        SELECT
            :v_action       AS action,
            :v_change_id    AS change_id,
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
        WHERE source_system_id = :p_source_system_id
    );

    RETURN TABLE(v_result);

END;
$$;


-- ============================================================
-- SECTION 3: Usage examples
-- ============================================================

-- Insert new subsidiary
CALL <DATABASE>.ref.sp_upsert_entity(
    'ENT-005', 'Acme Southwest LLC', 'ACME_SW', 'SUBSIDIARY', 'ENT-001',
    TRUE, '2026-01-01'::DATE, NULL,
    'Acquired January 2026', 'data_engineering'
);

-- Insert new source system
CALL <DATABASE>.ref.sp_upsert_source_system(
    'stripe', 'Stripe Payments', 'API',
    'Payment transactions and subscription data from Stripe',
    'ENT-001', 'Finance Engineering', 'finance-eng@company.com',
    'REST_API', 'PROD', TRUE, '2026-03-20'::DATE, NULL, NULL,
    'data_engineering'
);
