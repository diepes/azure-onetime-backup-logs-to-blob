#/usr/bin/bash
echo "Start $0"
# debug
set -e
set -o pipefail
set -o nounset

# Get vars
env=${1:-dev}
table_name="$2"
t_beginning="$3"  #->t_old
t_start_input="$4"  #->t_start
file_name="$5"
workspace_id="$6"
table_record_count_expected="$7"
table_record_count_downloaded=0

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

err_redo=0  #Count # of failed queries

if [[ -f "$file_name.uploadDone" ]]; then
    echo "ERROR found \"$file_name.uploadDone\""
    exit 1
fi

if [[ $table_record_count_expected -eq 0 ]]; then
        echo "    record_count=$table_record_count_expected - empty file - no need to query."  | tee -a $download_path/_log.txt
        touch "$file_name.gz"
        # now still have to check upload.
fi

if [[ ! -f "$file_name.gz" ]]; then
    echo "#   Downloading data for table: \"$table_name\"" | tee -a $download_path/_log.txt
    # Retrieve table with small time intervals.
    # Seen tables of 80k max records, file size of 130M max
    # Use 10min interval, agressive.
    # v2 reverste time start at now an work back to (now - days ago)
    ## Do multiple queries.
    #t_step_minimum=$(( 60 * 60 * 24 ))  # 86400s = 1 day
    t_step_minimum=$(( 60 * 20 ))  # 10min 60x10 - start here and increase.
    t_step=$t_step_minimum
    t_now=$(date +%s)
    ## input var t_beginning=$(( t_now - ($days_back * 86400) ))
    t_start=$t_start_input
    t_old=$((t_now - $t_step))
    split_cnt=0
    table_record_count=0  # num of records retrieved from query
    block_step_inc_cnt=0  # when reducing step block inc for this many rounds.
    # Working from old to new
    while [[ $t_start -gt $t_beginning ]]; do
        split_cnt=$(($split_cnt+1))
        table_record_count_previous=$table_record_count
        t_old_str="$(date -d @$t_old +"%Y-%m-%dT%H:%M:%SZ")"
        t_start_str="$(date -d @$t_start +"%Y-%m-%dT%H:%M:%SZ")"
        t_str="todatetime('$t_old_str') .. todatetime('$t_start_str')"
        t_str_display="'$t_old_str'..'$t_start_str'"
        if [[ $block_step_inc_cnt -gt 0 ]]; then
            block_step_inc_cnt=$(( $block_step_inc_cnt -1 ))
        fi
        query="$table_name |where TimeGenerated between ($t_str) |sort by TimeGenerated asc"
        echo "#    debug --analytics-query \"$query\"  t_diff_old=$(( $t_start - $t_old )) t_diff_beg=$(( $t_start - $t_beginning ))" | tee -a $download_path/_log.txt
        file_name_split="${file_name}.split.${split_cnt}"
        echo "running ... az monitor log-analytics query --analytics-query \"$query\"" >> $download_path/_error_query.txt
        table_record_count=$( \
            az monitor log-analytics query \
                --workspace "$workspace_id" \
                --analytics-query "$query" \
                --output json  2>> $download_path/_error_query.txt \
            | tee -a $file_name_split | jq '. | length'
            )
        rc=$?
        if [[ $rc -ne 0 ]]; then
            err_redo=$(( $err_redo + 1 ))
            echo "Error rc=$rc az monitor query - see $download_path/_error_query.txt - err_redo=$err_redo" | tee -a $download_path/_log.txt | tee -a $download_path/_error_query.txt
            t_old=$t_start  #Reset to start
            rm $file_name_split
            touch "${file_name_split}.REDO-DEL.${err_redo}"
            echo "#     ERROR Reset t_old to t_start=$t_start ,touch empty ${file_name_split}.REDO-DEL.${err_redo} RETRY ..." | tee -a $download_path/_log.txt | tee -a $download_path/_error_query.txt
            #exit 1
            continue
        fi
        est_cnt_left=$( echo "($t_old - $t_beginning)/$t_step/1" | bc)
        t_back_from_now_days=$( echo "($t_now - $t_old)/60/60/24" | bc)
        file_size=$( ls -l $file_name_split | awk '{print  $5}' )
        file_size_mb=$( echo "$file_size /1000/1000/1" | bc)
        rec_left=$(( $table_record_count_expected - $table_record_count_downloaded ))
        echo "#  rc=$rc table \"$table_name\" rec#=$table_record_count(${file_size_mb}MB) split=$split_cnt(+${est_cnt_left}) step=${t_step}s($( echo "$t_step /60/60/1" | bc)h) rec($rec_left) @-${t_back_from_now_days}/${days_back}days $t_str_display" | tee -a $download_path/_log.txt
        if [[ $table_record_count -gt 40000 ]] || [[ $file_size -gt 45000000 ]]; then
            if [[ $file_size -gt $(( 90 * 1000 * 1000)) ]]; then
                echo "#    ERROR REDO as file_size=$file_size and table_record_count=$table_record_count > 40000 might be losing logs, reduce step time ! $file_name_split" | tee -a $download_path/_log.txt
                # Try recovery run again.
                # Reset t_step to guessed 10MB mark
                t_step=$( echo "$t_step * (10 * 1000 * 1000)/$file_size /1 +1" | bc)
                t_old=$t_start  #Reset to start
                rm $file_name_split
                err_redo=$(( $err_redo + 1 ))
                touch "${file_name_split}.REDO-DEL.${err_redo}"
                echo "#     Reset t_old to t_start=$t_start ,touch empty ${file_name_split}.REDO-DEL.${err_redo}  new reduced t_step=$t_step"
                block_step_inc_cnt=$(( $block_step_inc_cnt + 10)) #Block increase for next 10 steps
                #exit 1
                continue
            elif [[ $file_size -gt $(( 55 * 1000 * 1000)) ]]; then
                echo "#   WARNING file_size of last split $file_size reduce t_step=${t_step}s by 25%"
                t_step=$( echo "$t_step * 0.75/1 +1" | bc)
                block_step_inc_cnt=$(( $block_step_inc_cnt + 2)) #Block increase for next 10 steps
            else
                echo "#   slowdown table_record_count=$table_record_count > 45k but file_size=$file_size < 50MB, reduce t_step=${t_step}s by 10%"
                t_step=$( echo "$t_step * 0.9/1 +1" | bc)
            fi
        # check if we shold increase t_step size
        elif [[ $table_record_count -lt 30000 ]] && [[ $t_step -lt $(( 60 * 60 * 24 )) ]] && [[ $file_size -lt 45000000 ]]; then
            if [[ $table_record_count -gt $table_record_count_previous ]] ; then
                echo "#    Skip speedup inc rec cnt > previous rec count, increasing. block_step_inc_cnt=$block_step_inc_cnt"
                if [[ $( echo "( $table_record_count - $table_record_count_previous ) /1000/1" | bc) -gt 5 ]]; then
                    echo "#       slowdown records increase so fast >5k lets slow down 5%"
                    t_step=$( echo "$t_step * 0.95/1 +1" | bc)
                fi
            elif [[ $block_step_inc_cnt -eq 0 ]]; then
                t_step_old=$t_step
                t_step=$( echo "$t_step * 1.1/1 +1" | bc)
                echo "#    speedup 10% t_step_old $t_step_old to $t_step as cnt=$table_record_count < 30k && step<1d"
            fi
        fi
        if [[ $table_record_count -eq 0 ]]; then
            echo "#    no records for $table_name cnt=$table_record_count remove split file $file_name_split"
            rm $file_name_split
        fi
        table_record_count_downloaded=$(( $table_record_count_downloaded + $table_record_count ))
        t_start=$t_old  # move back in time
        t_old=$(( $t_old - $t_step ))
        if [[ $t_old -lt $t_beginning ]]; then
            t_old=$t_beginning
        fi
    done # reached t_beginning

    echo "#    DONE downloading table cnt#=$table_record_count_downloaded == $table_record_count_expected now merge split files into $file_name.gz" | tee -a $download_path/_log.txt
    if [[ $table_record_count_downloaded -ne $table_record_count_expected ]]; then
        echo "   Error wrong table_record_count_downloaded=$table_record_count_downloaded table_record_count_expected=$table_record_count_expected" | tee -a $download_path/_log.txt
        exit 1
    fi
    cat $file_name.split.* | jq -s 'add' | gzip > $file_name.gz
    rm $file_name.split.*
    echo "## Downloaded table: '$table_name' $table_record_count_downloaded records" | tee -a $download_path/_log.txt
else

    echo "## file already exists $file_name proceed to upload blob ..." | tee -a $download_path/_log.txt
    table_record_count_downloaded="$(zcat ${file_name}.gz | jq '. | length' )"
fi

f="$file_name.gz"
    echo "## Start upload to $container_name f=${f} cnt#=$table_record_count_downloaded == $table_record_count_expected" | tee -a $download_path/_log.txt
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
            --type block --tier "hot" \
            --metadata "records_count=$table_record_count_downloaded"
    rc=$?
    if [[ $rc -ne 0 ]]; then
        echo "## Error with blob upload - exit"
        exit 1
    fi
    t="$(ElapsedMinutes $(( $(date +%s) - ${time_start} )) " minutes")"
    echo "## ${f##*/} uploaded to blob container. empty file ${t}" | tee -a $download_path/_log.txt
    rm ${f}
    touch $file_name.uploadDone
#done
