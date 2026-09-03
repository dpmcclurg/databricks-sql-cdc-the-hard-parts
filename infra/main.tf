terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {
    resource_group {
      # Delete the RG directly via the Azure API, clearing any nested resources
      # (e.g. the Databricks workspace mid-async-delete). Safe here: disposable testbed.
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id = var.subscription_id
}

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# Generate an admin password if none supplied. Never commit a real one.
resource "random_password" "sql_admin" {
  count            = var.sql_admin_password == "" ? 1 : 0
  length           = 24
  special          = true
  override_special = "!#$%*-_"
}

locals {
  sql_admin_password = var.sql_admin_password != "" ? var.sql_admin_password : random_password.sql_admin[0].result
}

resource "azurerm_mssql_server" "sql" {
  name                          = var.sql_server_name
  resource_group_name           = azurerm_resource_group.rg.name
  location                      = azurerm_resource_group.rg.location
  version                       = "12.0"
  administrator_login           = var.sql_admin_login
  administrator_login_password  = local.sql_admin_password
  minimum_tls_version           = "1.2"
  public_network_access_enabled = true
}

# S3 is the lowest DTU (Standard) tier that supports Change Data Capture.
resource "azurerm_mssql_database" "db" {
  name      = var.sql_database_name
  server_id = azurerm_mssql_server.sql.id
  sku_name  = "S3"
  collation = "SQL_Latin1_General_CP1_CI_AS"
}

# Open the firewall to all public IPs for the test. The Databricks gateway egresses through a
# NAT public IP, which the "Allow Azure services" (0.0.0.0) rule does NOT reliably cover, so a
# broad rule is the least-friction way to let the gateway connect. Acceptable here: the server
# holds only synthetic, disposable data. Set allow_all_ips = false to tighten (then add the
# workspace egress IP and client_ip yourself).
resource "azurerm_mssql_firewall_rule" "allow_all" {
  count            = var.allow_all_ips ? 1 : 0
  name             = "AllowAllForTest"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "255.255.255.255"
}

# Optional: your workstation IP, for running the SQL setup scripts locally.
resource "azurerm_mssql_firewall_rule" "client" {
  count            = var.client_ip == "" ? 0 : 1
  name             = "ClientIP"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = var.client_ip
  end_ip_address   = var.client_ip
}

# Premium workspace: required for Unity Catalog and serverless pipelines.
resource "azurerm_databricks_workspace" "dbx" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
}
