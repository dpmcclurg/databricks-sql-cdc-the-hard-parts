-- Run against the cdctest database (AFTER 03_connector_login.sql against master):
--   sqlcmd -S $SRV -d cdctest -U sqladmin -P "$PW" -i sql/04_connector_user.sql
--
-- Maps the server login to a database user and grants the reads the connector needs.

-- Drop any prior user of this name (e.g. a contained user from an earlier attempt) first.
DROP USER IF EXISTS lakeflow_connector;
GO
CREATE USER lakeflow_connector FOR LOGIN lakeflow_connector;
GO

ALTER ROLE db_datareader ADD MEMBER lakeflow_connector;
GO

-- Read the CDC change tables and call the CDC functions.
GRANT SELECT  ON SCHEMA::cdc TO lakeflow_connector;
GRANT EXECUTE ON SCHEMA::cdc TO lakeflow_connector;
GO

-- Confirm the exact grant set against the current Lakeflow SQL Server connector docs.
