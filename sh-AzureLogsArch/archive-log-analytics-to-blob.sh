#/usr/bin/bash
env=${1:-dev}
echo "## env=${env}"
if [[ -f config-${env}.sh ]] ; then
    source config-${env}.sh
else
    source ../config-${env}.sh
fi
##
# Functions
##
time_start=$(date +%s)
function ElapsedMinutes() {
    # Prints lapsed time in minutes, uses $1 can append $2
    t=$( awk -v t="${1:-$(( $(date +%s) - ${time_start} ))}" -v m="${2:-}" 'BEGIN {printf("%.2f%s", t / 60.0, m )}' )
    echo "$t"
}
##

# debug
set -e
set -o pipefail
set -o nounset

# Ensure temp dir exists
mkdir -p $download_path
echo "## Start $0 $(date -Iseconds)" | tee $download_path/_log.txt
# cleanout old 0byte files to retry download , skip empty []
find $download_path/* -type f -maxdepth 0 -size -3c -delete

# Log in to Azure
if az account list > /dev/null;
then
    echo "# Already logged into Azure"
else
    az login
fi


# Set the active subscription
az account set --subscription "$az_subscription"

# Get the Log Analytics workspace ID
workspace_id=$( az monitor log-analytics workspace show \
                    --resource-group $storage_rg \
                    --workspace-name "$workspace_name" \
                    --query customerId \
                    -o tsv \
                    | tee -a $download_path/_log.txt)

echo "## Got workspace_id=$workspace_id" | tee -a $download_path/_log.txt

# Get a list of tables in the workspace
echo "## Start retrieve table names." | tee -a $download_path/_log.txt
time table_names=$( \
    az monitor log-analytics workspace table list \
        --resource-group $storage_rg \
        --workspace-name $workspace_name \
        --output json \
    | jq -r ".[] | .name" \
    | tee $download_path/_info-table_names_and_details.txt
    )

table_l=$( echo "$table_names" | wc -l |  tr -d ' ')
if [[ $table_l -lt 10 ]]; then
    echo "#Error only found $table_l tables ?"
    exit 1
fi
echo "## Found $table_l tables" | tee -a $download_path/_log.txt

# Loop through each table and download its data
##for table_name in $table_names; do
table_c=0
echo "$table_names" | while read table_name ; do
    table_c=$((table_c+1))
    ElapsedMinutes $(( $(date +%s) - ${time_start} )) " minutes" | tee -a $download_path/_log.txt
    echo "## Downloading data for table: $table_c/$table_l \"$table_name\"" | tee -a $download_path/_log.txt
    # Get the data for the table
    if [[ "OmsCustomerProfileFact ReservedCommonFields" == *"${table_name}"* ]]; then
        echo "## Skip faulty table $table_name ..." | tee -a $download_path/_log.txt
        continue
    fi
    file_name="${download_path}/${table_name}_$( date +"%Y-%m-%d" )_${days_back}d.json"
    if [[ -f "$file_name" ]]; then
        echo "## Skip table $table_name, file already exists $file_name ..." | tee -a $download_path/_log.txt
        continue
    fi

    set +e
    table_record_count=$( \
        az monitor log-analytics query \
            --workspace "$workspace_id" \
            --analytics-query "${table_name}  |where TimeGenerated >= ago(${days_back}d)" \
            --output json  2> $download_path/_error_query.txt \
        | tee $file_name | jq '. | length'
        )
    rc=$?
    if [[ $rc != 0 ]]; then
        echo "# rc=$rc ERROR $( cat $download_path/_error_query.txt) "
        if cat $download_path/_error_query.txt | grep -q "Response size too large" ; then
            echo "# table $table_name got 'Response size too large', download day by day" | tee -a $download_path/_log.txt
            echo > $file_name
            ## Do multiple queries.
            t_step=86400 # Start 1 day in seconds
            t_now=$(date +%s)
            t_old=$(( t_now - ($days_back * 86400) ))
            t_start=$(( $t_old + $t_step ))
            while [[ $t_old -lt $t_now ]]; do
                t_old_str="$(gdate -d @$t_old +"%Y-%m-%dT%H:%M:%SZ")"
                t_start_str="$(gdate -d @$t_start +"%Y-%m-%dT%H:%M:%SZ")"
                query="$table_name |where TimeGenerated between (todatetime('$t_old_str') .. todatetime('$t_start_str'))"
                echo "# debug --analytics-query \"$query\"  diff=$(( $t_now - $t_old ))"
                table_record_count=$( \
                    az monitor log-analytics query \
                        --workspace "$workspace_id" \
                        --analytics-query "$query" \
                        --output json  2> $download_path/_error_query.txt \
                    | tee -a $file_name.split | jq '. | length'
                    )
                    echo "    table_record_count=${table_record_count}"
                echo "# table $table_name downloaded $table_record_count for interval t_old:$t_old_str t_start:$t_start_str" | tee -a $download_path/_log.txt
                t_old=$t_start
                t_start=$(( $t_start + $t_step ))
                if [[ $t_start -gt $t_now ]]; then
                    t_start=$t_now
                fi
            done
            echo "Debug - done getting table bit by bit now merge"
            cat $file_name.split | jq --slurp add > $file_name
        else
            exit 1
        fi #Response to large
    fi #err

    echo "## Downloaded table: $table_c/$table_l '$table_name' $table_record_count records" >> $download_path/_log.txt

done


echo "# Start Upload's to the blob container"

for f in $download_path/*.json; do
    echo "## Start upload to $container_name f=${f}" | tee -a $download_path/_log.txt
    if [[ "$container_sas_token" == "" ]]; then
        auth="--auth-mode login"
        echo "# No container_sas_token set in config-${env}.sh using $auth"
    else
        auth="--sas-token $container_sas_token"
        echo "# Using container_sas_token to auth to blob container $container_name"
    fi
    az storage blob upload \
            ${auth} \
            --account-name "$storage_account_name" \
            --container-name "$container_name" \
            --file "${f}" \
            --name "${f##*/}" \
            --type block \
            --type block --tier "hot"

    t="$(ElapsedMinutes $(( $(date +%s) - ${time_start} )) " minutes")"
    echo "## ${f##*/} uploaded to blob container. ${t}" | tee -a $download_path/_log.txt
done

t="$(ElapsedMinutes $(( $(date +%s) - ${time_start} )) " minutes")"
echo "## Done. TheEnd. ${t}" | tee -a $download_path/_log.txt
