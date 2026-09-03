output "sql_server_fqdn" {
  description = "Azure SQL server host, for the UC connection and the sim CLI."
  value       = azurerm_mssql_server.sql.fully_qualified_domain_name
}

output "sql_database_name" {
  value = azurerm_mssql_database.db.name
}

output "sql_admin_login" {
  value = var.sql_admin_login
}

output "sql_admin_password" {
  description = "Admin password (generated if not supplied). Retrieve with: terraform output -raw sql_admin_password"
  value       = local.sql_admin_password
  sensitive   = true
}

output "databricks_workspace_url" {
  description = "Use with: databricks auth login --host https://<this>"
  value       = "https://${azurerm_databricks_workspace.dbx.workspace_url}"
}

output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}
