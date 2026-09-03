-- Run against the MASTER database of the Azure SQL logical server:
--   sqlcmd -S $SRV -d master -U sqladmin -P "$PW" -v CONNECTOR_PW="<strong-password>" -i sql/03_connector_login.sql
--
-- The Lakeflow UC connection has no "database" option, so the gateway opens its first
-- connection pool against master, THEN reads each source database per-object. So the login
-- needs (1) to exist at the server level and (2) a user in master so it can actually connect
-- there. A server-level LOGIN alone is not enough: without a master user the gateway fails with
-- SOURCE_AUTHENTICATION_FAILURE -> 'server principal "lakeflow_connector" is not able to access
-- the database "master"'. A contained database user cannot authenticate against master at all.
-- Pair with 04_connector_user.sql (run in cdctest) for the actual CDC read grants.
--
-- Password note: use an ALPHANUMERIC password (no !, $, ;, {, } etc.). Shell/sqlcmd -v
-- substitution and connection-string parsing handle special chars inconsistently and can
-- silently store a different password on one side, producing a confusing auth failure.

CREATE LOGIN lakeflow_connector WITH PASSWORD = N'$(CONNECTOR_PW)';
GO

-- User in master so the login can open its initial connection pool there (CONNECT only; no
-- data access in master is needed).
CREATE USER lakeflow_connector FOR LOGIN lakeflow_connector;
GO
