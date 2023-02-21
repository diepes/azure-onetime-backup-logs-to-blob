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
mkdir -p $download_path/oldsplit
echo "## Start $0 $(date -Iseconds)" | tee $download_path/_log.txt | tee $download_path/_error_query.txt
# cleanout old 0byte files to retry download , skip empty []
#find $download_path/* -type f -maxdepth 0 -size -3c -delete
find $download_path/* -maxdepth 0 -type f -name *.split -delete
find $download_path/* -maxdepth 0 -type f -name *.json.split.* -delete
find $download_path/* -maxdepth 0 -type f -name *.json.*.split.* -delete  # e.g. .json.d90.split.69
find $download_path/oldsplit/  -maxdepth 0 -type f -name *.json.split.* -delete
# not .json.uploadDone

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
table_names=$( \
    az monitor log-analytics workspace table list \
        --resource-group $storage_rg \
        --workspace-name $workspace_name \
        --output json \
    | jq -r "sort_by(.name) | .[] | .name" \
    | tee $download_path/_info-table_names_and_details.txt
    )

table_l=$( echo "$table_names" | wc -l |  tr -d ' ')
if [[ $table_l -lt 10 ]]; then
    echo "#Error exit only found $table_l tables ?"
    exit 1
fi
echo "## Found $table_l tables" | tee -a $download_path/_log.txt

# Loop through each table and download its data
##for table_name in $table_names; do
table_c=0
err_redo=0  #Count # of failed queries
echo "$table_names" | while read table_name ; do
    file_name="${download_path}/${table_name}_$( date +"%Y-%m-19" )_${days_back}d.json"
    table_c=$((table_c+1))
    ElapsedMinutes $(( $(date +%s) - ${time_start} )) " minutes" | tee -a $download_path/_log.txt
    echo "# START for table: $table_c/$table_l \"$table_name\"" | tee -a $download_path/_log.txt
    # Get the data for the table
    # Skip broken or useless(big) tables - Perf, AzureDiagnostics
    if [[ "OmsCustomerProfileFact ReservedCommonFields Perf AzureDiagnostics" == *"${table_name}"* ]]; then
        echo "#   Skip faulty table $table_name ..." | tee -a $download_path/_log.txt
        touch $file_name.SKIP_DOWNLOAD.json
        continue
    fi
    if [[ -f "$file_name.uploadDone" ]]; then
        echo "## Skip table $table_name, file exists $file_name.uploadDone ..." | tee -a $download_path/_log.txt
        continue
    fi

    table_record_count=$( \
        az monitor log-analytics query \
            --workspace "$workspace_id" \
            --analytics-query "${table_name}  |where TimeGenerated >= ago(${days_back}d) |summarize Count=count()" \
            --output json  2>> $download_path/_error_query.txt \
        | jq -r ".[0].Count"
        )
    rc=$?
    if [[ $rc -ne 0 ]]; then
        echo "# ERROR exit getting table_record_count for \"${table_name}\" " |tee -a $download_path/_log.txt |tee -a $download_path/_error_query.txt
        exit 1
    fi
    if [[ $table_record_count -lt 19000000 ]]; then
        split_per_day=false
    else
        split_per_day=true
    fi
    if [[ $table_record_count -eq 0 ]]; then
        echo "    record_count=$table_record_count - empty file - no need to query or upload."  | tee -a $download_path/_log.txt
        touch $file_name.uploadDone
        continue
    fi

    echo "#    table \"$table_name\" record_count=$table_record_count split_per_day=$split_per_day"
    t_now=$(date -d "$(date +%Y-%m-%d) 00:00:00" +%s)  # Start of today
    t_beginning=$(( t_now - ($days_back * 86400) ))
    t_start_input=$t_now
    if [[ "$split_per_day" == "false" ]]; then
            ${0%/*}/log-download-start-end-table.sh $env "$table_name" $t_beginning $t_start_input "$file_name" "$workspace_id" "$table_record_count"
    else
        echo "Split $table_name into days"
        for day_back in $(seq -w $days_back -1 1);
        do
            t_beginning=$(( t_now - (($day_back) * 86400) ))
            t_start_input=$(( t_now - (($day_back-1) * 86400) ))
            echo "    $table_name day=$day_back/$days_back"
            ${0%/*}/log-download-start-end-table.sh $env "$table_name" $t_beginning $t_start_input "$file_name.d${day_back}" "$workspace_id" "$table_record_count"
        done
    fi
done

t="$(ElapsedMinutes $(( $(date +%s) - ${time_start} )) " minutes")"
echo "## Done. TheEnd. ${t}" | tee -a $download_path/_log.txt
