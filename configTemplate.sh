#Example: copy to config-dev.sh and
# PVT dont check into git.
echo "## DEV"
az_subscription="abcdefgh-1234-5678-9abc-def0-1234abcd56"
days_back=90
workspace_name="dev-env-wksp"
storage_account_name="devblob"
storage_rg="dev-rg"
container_sas_token="sp=acwl&st=2023-02-18T05:13:13Z&se=2023-02-18T13:13:13Z&spr=https&sv=2021-06-08&sr=c&sig=<secret>"
container_name="backups"
download_path="/tmp/log-analytics-table-data-dev"
