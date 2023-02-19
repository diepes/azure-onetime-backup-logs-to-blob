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
echo "## Start $0 $(date -Iseconds)" | tee $download_path/_log.txt
# cleanout old 0byte files to retry download , skip empty []
find $download_path/* -type f -maxdepth 0 -size -3c -delete
find $download_path/* -type f -maxdepth 0 -name *.split -delete
find $download_path/* -type f -maxdepth 0 -name *.json.split.* -delete
find $download_path/oldsplit/ -type f -maxdepth 0 -name *.json.split.* -delete

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
err_redo=0  #Count # of failed queries
echo "$table_names" | while read table_name ; do
    file_name="${download_path}/${table_name}_$( date +"%Y-%m-19" )_${days_back}d.json"
    table_c=$((table_c+1))
    ElapsedMinutes $(( $(date +%s) - ${time_start} )) " minutes" | tee -a $download_path/_log.txt
    echo "#   Downloading data for table: $table_c/$table_l \"$table_name\"" | tee -a $download_path/_log.txt
    # Get the data for the table
    # Skip broken or useless(big) tables
    if [[ "OmsCustomerProfileFact ReservedCommonFields Perf" == *"${table_name}"* ]]; then
        echo "#   Skip faulty table $table_name ..." | tee -a $download_path/_log.txt
        touch $file_name.SKIP_DOWNLOAD.json
        continue
    fi
    if [[ -f "$file_name" ]]; then
        echo "## Skip table $table_name, file already exists $file_name ..." | tee -a $download_path/_log.txt
        continue
    fi

    table_record_count=$( \
        az monitor log-analytics query \
            --workspace "$workspace_id" \
            --analytics-query "${table_name}  |where TimeGenerated >= ago(${days_back}d) |summarize Count=count()" \
            --output json  2> $download_path/_error_query.txt \
        | jq -r ".[0].Count"
        )
    rc=$?
    if [[ $rc -ne 0 ]]; then
        echo "# ERROR getting table_record_count for \"${table_name}\" "
        exit 1
    fi
    if [[ $table_record_count -lt 40000 ]]; then
        echo "# table $table_name has \"$table_record_count\" < 40k records get all at once" | tee -a $download_path/_log.txt
        set +e
        table_record_count_jq=$( \
            az monitor log-analytics query \
                --workspace "$workspace_id" \
                --analytics-query "${table_name}  |where TimeGenerated >= ago(${days_back}d) |sort by TimeGenerated asc" \
                --output json  2> $download_path/_error_query.txt \
            | tee $file_name | jq '. | length'
            )
        rc=$? ; set -e
        if [[ $table_record_count_jq -ne $table_record_count ]]; then
            echo "# ERROR full download has different record count than Count query :(" | tee -a $download_path/_log.txt
            exit1
        fi
        if [[ $rc -ne 0 ]]; then
            echo "# ERROR getting full record set for \"${table_name}\" "
            if [[ -f $file_name ]]; then
                mv $file_name $file_name.debug.delme.split
            fi
            exit 1
        fi
    else # count > 40k do in chunks
        echo "#    table \"$table_name\" large record_count=$table_record_count fall back to small interval retrieval"
        # Retrieve table with small time intervals.
        # Seen tables of 80k max records, file size of 130M max
        # Use 10min interval, agressive.
        # v2 reverste time start at now an work back to (now - days ago)
            ## Do multiple queries.
            t_step_minimum=$(( 60 * 60 * 24 ))  # 86400s = 1 day
            t_step_minimum=$(( 60 * 20 ))  # 10min 60x10 - start here and increase.
            t_step=$t_step_minimum
            t_now=$(date +%s)
            t_beginning=$(( t_now - ($days_back * 86400) ))
            #t_old=$(( t_now - ($days_back * 86400) ))
            #t_start=$(( $t_old + $t_step ))
            t_start=$t_now
            t_old=$((t_now - $t_step))
            split_cnt=0
            table_record_count=0  # num of records retrieved from query
            block_step_inc_cnt=0  # when reducing step block inc for this many rounds.
            while [[ $t_start -gt $t_beginning ]]; do
                split_cnt=$(($split_cnt+1))
                table_record_count_previous=$table_record_count
                t_old_str="$(gdate -d @$t_old +"%Y-%m-%dT%H:%M:%SZ")"
                t_start_str="$(gdate -d @$t_start +"%Y-%m-%dT%H:%M:%SZ")"
                t_str="todatetime('$t_old_str') .. todatetime('$t_start_str')"
                t_str_display="'$t_old_str'..'$t_start_str'"
                if [[ $block_step_inc_cnt -gt 0 ]]; then
                    block_step_inc_cnt=$(( $block_step_inc_cnt -1 ))
                fi
                query="$table_name |where TimeGenerated between ($t_str) |sort by TimeGenerated asc"
                #echo "#    debug --analytics-query \"$query\"  diff=$(( $t_start - $t_old ))"
                file_name_split="${file_name}.split.${split_cnt}"
                table_record_count=$( \
                    az monitor log-analytics query \
                        --workspace "$workspace_id" \
                        --analytics-query "$query" \
                        --output json  2> $download_path/_error_query.txt \
                    | tee -a $file_name_split | jq '. | length'
                    )
                rc=$?
                est_cnt_left=$( echo "($t_old - $t_beginning)/$t_step/1" | bc)
                t_back_from_now_days=$( echo "($t_now - $t_old)/60/60/24" | bc)
                file_size=$( ls -l $file_name_split | awk '{print  $5}' )
                file_size_mb=$( echo "$file_size /1000/1000/1" | bc)
                echo "#  rc=$rc table \"$table_name\" rec#=$table_record_count(${file_size_mb}MB) split=$split_cnt(+${est_cnt_left}) step=${t_step}s($( echo "$t_step /60/60/1" | bc)h) @-${t_back_from_now_days}/${days_back}days $t_str_display" | tee -a $download_path/_log.txt
                if [[ $table_record_count -gt 40000 ]] || [[ $file_size -gt 45000000 ]]; then
                    if [[ $file_size -gt $(( 90 * 1000 * 1000)) ]]; then
                        echo "#    ERROR REDO as file_size=$file_size and table_record_count=$table_record_count > 40000 might be losing logs, reduce step time ! $file_name_split" | tee -a $download_path/_log.txt
                        # Try recovery run again.
                        # Reset t_step to guessed 10MB mark
                        t_step=$( echo "$t_step * (10 * 1000 * 1000)/$file_size /1 +1" | bc)
                        t_old=$t_start  #Reset to start
                        rm $file_name_split
                        touch "${file_name}.split.${split_cnt}.REDO-DEL"
                        err_redo=$(( $err_redo + 1 ))
                        echo "#     Reset t_old to t_start=$t_start ,touch empty ${file_name}.split.REDO-DEL.${split_cnt}  new reduced t_step=$t_step"
                        block_step_inc_cnt=$(( $block_step_inc_cnt + 10)) #Block increase for next 10 steps
                        #exit 1
                    elif [[ $file_size -gt $(( 55 * 1000 * 1000)) ]]; then
                        echo "#   WARNING file_size of last split $file_size reduce t_step=${t_step}s by 25%"
                        t_step=$( echo "$t_step * 0.75/1 +1" | bc)
                        block_step_inc_cnt=$(( $block_step_inc_cnt + 2)) #Block increase for next 10 steps
                    else
                        echo "#   WARNING Ok table_record_count=$table_record_count > 40k but file_size=$file_size < 50MB, reduce t_step=${t_step}s by 10%"
                        t_step=$( echo "$t_step * 0.9/1 +1" | bc)
                    fi
                # check if we shold increase t_step size
                elif [[ $table_record_count -lt 30000 ]] && [[ $t_step -lt $(( 60 * 60 * 24 )) ]] && [[ $file_size -lt 45000000 ]]; then
                    if [[ $table_record_count -gt $table_record_count_previous ]] ; then
                        echo "#    Skip speedup inc rec cnt > previous rec count, increasing. block_step_inc_cnt=$block_step_inc_cnt"
                        if [[ $( echo "( $table_record_count - $table_record_count_previous ) /1000/1" | bc) -gt 5 ]]; then
                            echo "#       records increase so fast >5k lets slow down 5%"
                            t_step=$( echo "$t_step * 0.95/1 +1" | bc)
                        fi
                    elif [[ $block_step_inc_cnt -eq 0 ]]; then
                        t_step_old=$t_step
                        t_step=$( echo "$t_step * 1.1/1 +1" | bc)
                        echo "#    speedup 10% t_step_old $t_step_old to $t_step as cnt=$table_record_count < 30k && step<1d"
                    fi
                fi
                if [[ $table_record_count -eq 0 ]]; then
                    echo "#    no records for $table_name cnt=$table_record_count remove split file"
                    rm $file_name_split
                fi

                #t_old=$t_start
                t_start=$t_old  # move back in time
                #t_start=$(( $t_start + $t_step ))
                t_old=$(( $t_old - $t_step ))
                #if [[ $t_start -gt $t_now ]]; then
                if [[ $t_old -lt $t_beginning ]]; then
                    #t_start=$t_now
                    t_old=$t_beginning
                fi
            done
            echo "#    Debug - done getting table bit by bit now merge split files into $file_name" | tee -a $download_path/_log.txt
            #cat $file_name.split.* | jq --slurp 'add | sort_by(.TimeGenerated)' > $file_name
            # as query sorts recores -s and no sort should be ok.
            cat $file_name.split.* | jq -s 'add' > $file_name
            mv $download_path/*.json.split.* $download_path/oldsplit/
    fi #err and split time
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
