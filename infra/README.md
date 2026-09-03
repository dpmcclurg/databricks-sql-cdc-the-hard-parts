# infra (Terraform)

Provisions the Azure side only: a resource group, an Azure SQL Database at the S3 DTU tier (lowest tier that supports CDC), and a Premium Databricks workspace. Databricks objects (catalog, connection, pipelines, jobs) live in the DAB under `../databricks`, not here.

## Prerequisites

- `terraform` and `az` installed; `az login` completed.
- An Azure subscription ID.

## Use

```bash
cp terraform.tfvars.example terraform.tfvars   # then edit (server name must be globally unique)
terraform init
terraform plan
terraform apply
```

Read back what the next steps need:

```bash
terraform output sql_server_fqdn
terraform output sql_database_name
terraform output databricks_workspace_url
terraform output -raw sql_admin_password
```

## Notes

- `sku_name = "S3"` is deliberate: CDC on Azure SQL Database (DTU model) requires at least S3. Subcore tiers (Basic, S0 to S2) do not support CDC.
- Networking is minimal: public network access (default) plus one firewall rule. No VNet, peering, Private Link, or ExpressRoute; those are production / private / on-prem concerns and are not needed for a public-endpoint test. The default `allow_all_ips = true` opens the firewall to all public IPs, which is the reliable low-friction choice because the gateway egresses through a NAT public IP that the "Allow Azure services" rule does not reliably cover. Acceptable here (synthetic, disposable data); set `allow_all_ips = false` to tighten and manage rules yourself.
- The new workspace may need serverless compute enabled at the account level before the ingestion and silver pipelines can run serverless. Confirm in the account console.

## Teardown

```bash
terraform destroy
```
