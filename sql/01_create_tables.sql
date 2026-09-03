-- Sample vendor-ERP-like tables for the CDC testbed.
--
-- DELIBERATELY KEYLESS HEAPS WITH NULLABLE KEY FIELDS, to mirror a real source you do not administer:
--   * NO PRIMARY KEY, NO IDENTITY, NO UNIQUE constraint on any table.
--   * The assumed-key columns themselves are NULLABLE (hmy, and lease's property_hmy/tenant_hmy
--     are BIGINT NULL). This is the hard case: the field you would key on can be NULL,
--     and you do NOT administer the source, so you cannot fix it upstream.
-- hmy (and lease's pair) are plain app-assigned columns, not declared keys.
--
-- Consequence that the pipelines encode:
--   * Ingest: the connector needs no declared source PK, but you MUST specify the logical key
--     (table_configuration.primary_keys: [hmy]) or the all-columns bundled-key default drops
--     UPDATEs. See docs/experiments.md E11.
--   * Silver: SCD Type 1/2 requires a key, and it is never a source PK. It is an ASSUMED key
--     (hmy, defended with data quality), a CONSOLIDATED/composite key (lease), a TRANSFORMED
--     surrogate (content-hash, variant 06), or - when no key can be formed - none at all, in
--     which case only an append-only log is possible (variant 07).
--   * Because the assumed key is NULLABLE and uncontrolled, the DATA QUALITY layer (variant 05:
--     quarantine null/invalid keys + a cross-row uniqueness circuit breaker) is ESSENTIAL, not
--     optional. It is the only thing that makes an assumed key safe to build SCD on.
--
-- Table mix is deliberate: some date-less (reproduce NULL __cdc_timestamp_value), one with a
-- reliable modified-date, one with a composite (consolidated) key.
-- Run against the target Azure SQL Database.

IF OBJECT_ID('dbo.lease','U')    IS NOT NULL DROP TABLE dbo.lease;
IF OBJECT_ID('dbo.account','U')  IS NOT NULL DROP TABLE dbo.account;
IF OBJECT_ID('dbo.property','U') IS NOT NULL DROP TABLE dbo.property;
IF OBJECT_ID('dbo.tenant','U')   IS NOT NULL DROP TABLE dbo.tenant;
GO

-- account: DATE-LESS keyless heap. hmy is a plain NULLABLE column (assumed key downstream).
-- Reproduces the date-less case (sequence_by autodetects to LSN, timestamp NULL on snapshot rows).
CREATE TABLE dbo.account (
    hmy      BIGINT        NULL,           -- assumed key, nullable and unconstrained (the hard keyless case)
    name     NVARCHAR(200) NOT NULL,
    status   NVARCHAR(40)  NOT NULL,
    balance  DECIMAL(18,2) NOT NULL
);
GO

-- property: keyless heap that HAS a reliable modified-date. Shows the "good date" case for
-- sequence_by. hmy is still only a nullable assumed key (no constraint).
CREATE TABLE dbo.property (
    hmy         BIGINT        NULL,        -- assumed key, nullable and unconstrained
    name        NVARCHAR(200) NOT NULL,
    city        NVARCHAR(120) NOT NULL,
    sq_ft       INT           NOT NULL,
    modified_at DATETIME2     NOT NULL
);
GO

-- tenant: DATE-LESS keyless heap, high-churn target for the intra-interval burst test.
CREATE TABLE dbo.tenant (
    hmy     BIGINT        NULL,            -- assumed key, nullable and unconstrained
    name    NVARCHAR(200) NOT NULL,
    email   NVARCHAR(200) NOT NULL,
    credit  INT           NOT NULL
);
GO

-- lease: keyless heap whose logical identity is a CONSOLIDATED/composite key
-- (property_hmy, tenant_hmy) - assumed, nullable, not constrained - with a reliable modified-date.
-- Exercises composite primary_keys on the connector and composite keys= in silver.
CREATE TABLE dbo.lease (
    property_hmy BIGINT        NULL,       -- assumed composite key part, nullable
    tenant_hmy   BIGINT        NULL,       -- assumed composite key part, nullable
    rent         DECIMAL(18,2) NOT NULL,
    term_months  INT           NOT NULL,
    modified_at  DATETIME2     NOT NULL
);
GO
