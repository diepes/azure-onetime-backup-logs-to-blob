#Example: copy to config-dev.sh and update.
# PVT dont check into git.
echo "## DEV"
az_subscription="abcdefgh-1234-5678-9abc-def0-1234abcd56"
days_back=90
workspace_name="dev-env-wksp"
storage_account_name="devblob"
storage_rg="dev-rg"
container_name="backups"
download_path="/tmp/log-analytics-table-data-dev"
