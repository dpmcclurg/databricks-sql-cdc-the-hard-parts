-- Keyless-heap sandbox table for the data-quality and true-heap variants (05 assumed-key DQ,
-- 06 content-hash, 07 append-log). Kept SEPARATE from the main tables so the "break the assumed
-- key" demos (inject a NULL or duplicate hmy) never corrupt the variant 01-04 demo tables.
--
-- Mirrors the worst-case source table: a HEAP with NO primary key, NO identity, NO unique
-- constraint, and a NULLABLE hmy. You do not administer the source, so hmy really can
-- arrive NULL or duplicated - which is exactly why the DQ quarantine + circuit breaker exist.
--
-- Run AFTER 02_enable_cdc.sql (the database must already be CDC-enabled).

IF OBJECT_ID('dbo.account_nopk','U') IS NOT NULL DROP TABLE dbo.account_nopk;
GO

CREATE TABLE dbo.account_nopk (
    hmy      BIGINT        NULL,           -- assumed key: nullable, unconstrained, uncontrolled
    name     NVARCHAR(200) NOT NULL,
    status   NVARCHAR(40)  NOT NULL,
    balance  DECIMAL(18,2) NOT NULL
);
GO

-- @supports_net_changes = 0 is required (heap, no unique index). Lakeflow Connect ingests
-- all-changes rows; specify table_configuration.primary_keys:[hmy] on the ingestion object.
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo', @source_name = N'account_nopk',
    @role_name = NULL, @supports_net_changes = 0;
GO

-- Demo helpers (run by hand during the variant 05 walkthrough, not at setup):
--   Quarantine (Tier 1): inject an invalid assumed key ->
--     INSERT INTO dbo.account_nopk (hmy,name,status,balance) VALUES (NULL,'orphan','active',0);
--     INSERT INTO dbo.account_nopk (hmy,name,status,balance) VALUES (0,'zero-key','active',0);
--   Circuit breaker (Tier 2): inject a genuine duplicate assumed key ->
--     INSERT INTO dbo.account_nopk (hmy,name,status,balance)
--       SELECT hmy,name,status,balance FROM dbo.account_nopk WHERE hmy = 5;
