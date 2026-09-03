variable "subscription_id" {
  type        = string
  description = "Azure subscription ID to deploy into."
}

variable "location" {
  type        = string
  description = "Azure region. Keep SQL and Databricks in the same region."
  default     = "eastus"
}

variable "resource_group_name" {
  type    = string
  default = "rg-sqlcdc-testbed"
}

variable "sql_server_name" {
  type        = string
  description = "Globally unique Azure SQL logical server name."
  default     = "sqlcdc-testbed-srv"
}

variable "sql_database_name" {
  type    = string
  default = "cdctest"
}

variable "sql_admin_login" {
  type    = string
  default = "sqladmin"
}

variable "sql_admin_password" {
  type        = string
  description = "Admin password. Leave empty to auto-generate (see the sql_admin_password output). Never commit a real value."
  default     = ""
  sensitive   = true
}

variable "client_ip" {
  type        = string
  description = "Your workstation public IP, to allow running the SQL setup scripts locally. Empty to skip."
  default     = ""
}

variable "allow_all_ips" {
  type        = bool
  description = "Open the SQL firewall to all public IPs. Convenient and reliable for a throwaway test (the gateway egresses via a NAT public IP). Set false to tighten and manage rules yourself."
  default     = true
}

variable "databricks_workspace_name" {
  type    = string
  default = "sqlcdc-testbed-dbx"
}
