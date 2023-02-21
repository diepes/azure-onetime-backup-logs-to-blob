# azure-onetime-backup-logs-to-blob
script to retrieve logs from Azure log-analytics workspace and upload tables to blob storage account

## bash script - sh-AzureLogsArch

 * Working - 2023-02

#### sh-AzureLogsArch - local test
  1. source config-<env>.sh
  2. workspace_id=$( az monitor log-analytics workspace show --resource-group $storage_rg --workspace-name "$workspace_name" --query customerId -o tsv )
  3. table_name="VMConnection"
  4. query="$table_name |where TimeGenerated between (todatetime('2023-02-11T13:08:41Z') .. todatetime('2023-02-11T15:01:28Z')) |sort by TimeGenerated asc"
  5. az monitor log-analytics query --workspace "$workspace_id" --analytics-query "$query" --output json

## python script - rust-AzureLogsArch

 * /!\ Not working yet, only ls

## rust script - rust-AzureLogsArch

 * /!\ NOT Working - only ls
 * curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
 * rustc --version
 * rustc src/main.rs ; ./main
