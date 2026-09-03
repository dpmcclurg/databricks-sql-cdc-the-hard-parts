-- Enable Change Data Capture on the database and each table.
-- CDC (not Change Tracking) gives the LSN-based change tables that reproduce
-- __cdc_internal_value = LSN with __cdc_timestamp_value = NULL for date-less tables.
-- On Azure SQL Database there is no SQL Agent; capture/cleanup run on the managed scheduler.
-- Requires the database to be at least S3 (DTU model). Run as a user with db_owner.

EXEC sys.sp_cdc_enable_db;
GO

-- @supports_net_changes = 0 is REQUIRED here: these are keyless heaps (no primary key), and
-- net-changes mode needs a unique index. Lakeflow Connect consumes all-changes rows anyway.
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo', @source_name = N'account',
    @role_name = NULL, @supports_net_changes = 0;
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo', @source_name = N'property',
    @role_name = NULL, @supports_net_changes = 0;
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo', @source_name = N'tenant',
    @role_name = NULL, @supports_net_changes = 0;
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo', @source_name = N'lease',
    @role_name = NULL, @supports_net_changes = 0;
GO

-- Verify: one row per capture instance.
EXEC sys.sp_cdc_help_change_data_capture;
GO
