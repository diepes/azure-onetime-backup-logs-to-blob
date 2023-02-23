#!/usr/bin/bash
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


echo "enter $0 env=${env} table=$table_name file_name=file_name"
if [[ -f config-${env}.sh ]] ; then
    source config-${env}.sh
else
    source ../config-${env}.sh
fi
##
# Functions
##
elapsed_time_start=$(date +%s)
function ElapsedMinutes() {
    # Prints lapsed time in minutes, uses $1 can append $2
    t=$( awk -v t="${1:-$(( $(date +%s) - ${elapsed_time_start} ))}" -v m="${2:-}" 'BEGIN {printf("%.2f%s", t / 60.0, m )}' )
    echo "$t"
}
##

err_redo=0  #Count # of failed queries

if [[ -f "$file_name.uploadDone" ]]; then
    # We could get here for large tables, split per day.
    echo "SKIP found \"$file_name.uploadDone\"" | tee -a $download_path/_log.txt
    echo
    exit 0
fi

if [[ $table_record_count_expected -eq 0 ]]; then
        echo "    record_count=$table_record_count_expected - empty file - no need to query."  | tee -a $download_path/_log.txt
        touch "$file_name.gz"
        # now still have to check upload.
fi
function update_t_step () {
    # call with x% to inc/dec, 10th of seconds
        t_step_pct=${1:-10}
        t_step_temp=$t_step
        if [[ $block_step_inc_cnt -eq 0 ]] || [[ $t_step_pct -lt 0 ]]; then
            t_step=$( echo "$t_step * (100+${t_step_pct})/100/1 +1" | bc)
            echo "#        func:update_t_step t_step ${t_step_pct}%  $t_step_temp > $t_step  block_step_inc_cnt=$block_step_inc_cnt"
        else
            echo "#        func:update_t_step t_step ${t_step_pct}% inc BLOCKED  block_step_inc_cnt=$block_step_inc_cnt do nothing."
        fi
}
if [[ ! -f "$file_name.gz" ]]; then
    echo "#   Downloading data for table: \"$table_name\"" | tee -a $download_path/_log.txt
    # Retrieve table with small time intervals.
    # Seen tables of 80k max records, file size of 130M max
    # Use 10min interval, agressive.
    # v2 reverste time start at now an work back to (now - days ago)
    ## Do multiple queries.
    #t_step_minimum=$(( 60 * 60 * 24 ))  # 86400s = 1 day
    t_step_minimum=$(( 60 * 1 ))  # 10min (10th of min) 60x10 - start here and increase.
    t_step=$(( $t_step_minimum * 100 ))
    #t_now=$(date +%s)
    ## input var t_beginning=$(( t_now - ($days_back * 86400) ))
    t_start=$(( $t_start_input * 100 )) ## 10th of seconds
    t_old=$(($t_start - $t_step))
    split_cnt=0
    table_record_count=0  # num of records retrieved from query
    block_step_inc_cnt=0  # when reducing step block inc for this many rounds.
    # Working from old to new
    touch "${file_name}.split.0000"
    while [[ $t_start -gt $(( $t_beginning * 100 )) ]]; do
        split_cnt=$(($split_cnt+1))
        table_record_count_previous=$table_record_count
        # 10th of seconds
        t_o=$( echo "scale=3;$t_old/100"|bc)
        t_s=$( echo "scale=3;$t_start/100"|bc)
        t_diff=$( echo "scale=3;$t_s - $t_o"|bc)
        t_old_str="$(date -d @$t_o +"%Y-%m-%dT%H:%M:%S.%NZ")"
        t_start_str="$(date -d @$t_s +"%Y-%m-%dT%H:%M:%S.%NZ")"
        t_str="todatetime('$t_old_str') .. todatetime('$t_start_str')"
        t_str_display="'$t_old_str'..'$t_start_str'"
        if [[ $block_step_inc_cnt -gt 0 ]]; then
            block_step_inc_cnt=$(( $block_step_inc_cnt -1 ))
        fi
        #query="$table_name |where TimeGenerated between ($t_str) |sort by TimeGenerated asc"
        # between includes start and end, rather use > and <=
        query="$table_name |where TimeGenerated > todate($t_old_str) and TimeGenerated <= todate($t_start_str) |sort by TimeGenerated asc"
        file_name_split="${file_name}.split.$( printf "%04i" ${split_cnt} )"
        echo
        echo "START: $file_name_split    t_diff=$t_diff" | tee -a $download_path/_log.txt
        echo "#    query=\"$query\"" | tee -a $download_path/_log.txt
        #echo "#    Time Debug old $t_old > $t_o and $t_start > $t_s  date -d @$t_o +"%Y-%m-%dT%H:%M:%S.%NZ" = $t_old_str"
        ## echo "running ... az monitor log-analytics query --analytics-query \"$query\"" >> $download_path/_error_query.txt
        set +e
        table_record_count=$( \
            az monitor log-analytics query \
                --workspace "$workspace_id" \
                --analytics-query "$query" \
                --output json  2>> $download_path/_error_query.txt \
            | tee -a $file_name_split | jq '. | length'
            )
        rc=$?
        set -e
        if [[ $rc -ne 0 ]]; then
            err_redo=$(( $err_redo + 1 ))
            echo "#    ERROR rc=$rc az monitor query - see $download_path/_error_query.txt - err_redo=$err_redo" | tee -a $download_path/_log.txt | tee -a $download_path/_error_query.txt
            t_old=$t_start  #Reset to start
            table_record_count=0  # Set to 0 discard file.
            touch "${file_name_split}.REDO-DEL.${err_redo}.err"
            ##t_step=$( echo "$t_step * 0.01/1 +1" | bc)  #Reduce to 1% e.g. 5000s to 100sec
            update_t_step -99 #Reduce to 1% e.g. 5000s to 100sec
            block_step_inc_cnt=$(( $block_step_inc_cnt + 10)) #Block increase for next 10 steps
            echo "#        Reset t_old to t_start=$t_start ,touch empty ${file_name_split}.REDO-DEL.${err_redo} reduce t_step=$t_step RETRY ..." | tee -a $download_path/_log.txt | tee -a $download_path/_error_query.txt
            #exit 1
            #continue
        else
            est_cnt_left=$( echo "($t_old - $t_beginning *100)/$t_step/1" | bc)
            #t_back_from_now_days=$( echo "($t_start_input - $t_old)/60/60/24" | bc)
            file_size=$( ls -l $file_name_split | awk '{print  $5}' )
            file_size_mb=$( echo "$file_size /1000/1000/1" | bc)
            rec_left=$(( $table_record_count_expected - $table_record_count_downloaded ))
            echo "#    rc=$rc \"$table_name\" rec#=$table_record_count(${file_size_mb}MB) split=$split_cnt(+${est_cnt_left}) t_step=${t_step}($( echo "scale=1;$t_step /100/60/60/1" | bc)h) rec($rec_left)" | tee -a $download_path/_log.txt
            if [[ $table_record_count -gt 40000 ]] || [[ $file_size -gt 45000000 ]]; then
                if [[ $file_size -gt $(( 90 * 1000 * 1000)) ]]; then
                    t_step_pct=$( echo "-(1 - (10 * 1000 * 1000)/$file_size) *100 /1 +1" | bc)
                    echo "#    ERROR REDO as file_size=$file_size and t_step_pct=$t_step_pct table_record_count=$table_record_count > 40000 might be losing logs, reduce step time ! $file_name_split" | tee -a $download_path/_log.txt
                    # Try recovery run again.
                    # Reset t_step to guessed 10MB mark
                    update_t_step $t_step_pct #Reduce to 1% e.g. 5000s to 100sec
                    t_old=$t_start  #Reset to start
                    err_redo=$(( $err_redo + 1 ))
                    table_record_count=0  # Set to 0 discard file.
                    touch "${file_name_split}.REDO-DEL.${err_redo}.size"
                    echo "#        Reset t_old to t_start=$t_start ,touch empty ${file_name_split}.REDO-DEL.${err_redo}  new reduced $t_step_pct% t_step=$t_step"
                    block_step_inc_cnt=$(( $block_step_inc_cnt + 10)) #Block increase for next 10 steps
                    #exit 1
                    #continue
                elif [[ $file_size -gt $(( 55 * 1000 * 1000)) ]]; then
                    echo "#    WARNING file_size of last split $file_size reduce t_step=${t_step}s by 25%"
                    #t_step=$( echo "$t_step * 0.75/1 +1" | bc)
                    update_t_step -25 #Reduce 25%
                    block_step_inc_cnt=$(( $block_step_inc_cnt + 2)) #Block increase for next 10 steps
                else
                    echo "#    slowdown table_record_count=$table_record_count > 45k but file_size=$file_size < 50MB, reduce t_step=${t_step}s by 10%"
                    #t_step=$( echo "$t_step * 0.9/1 +1" | bc)
                    update_t_step -10 #Reduce 10%
                fi
            # check if we shold increase t_step size
            elif [[ $table_record_count -lt 30000 ]] && [[ $t_step -lt $(( 60 * 60 * 24 * 100)) ]] && [[ $file_size -lt 11000000 ]]; then

                if [[ $table_record_count -gt $table_record_count_previous ]] ; then
                    echo "#    Skip speedup inc rec cnt > previous rec count, increasing. block_step_inc_cnt=$block_step_inc_cnt"
                    if [[ $( echo "( $table_record_count - $table_record_count_previous ) /1000/1" | bc) -gt 5 ]]; then
                        echo "#        slowdown records increase so fast >5k lets slow down 5%"
                        #t_step=$( echo "$t_step * 0.95/1 +1" | bc)
                        update_t_step -5 #Reduce 5%
                        block_step_inc_cnt=$(( $block_step_inc_cnt + 2)) #Block increase for next 10 steps
                    fi
                else
                    t_step_old=$t_step
                    echo "#    speedup 10% t_step_old $t_step_old to $t_step as cnt=$table_record_count < 30k && step<1d file_size=$(( $file_size / 1000 / 1000 )) < 11MB"
                    update_t_step 10 #Increase 10%
                fi
            fi
        fi  # $rc != 0
        if [[ $table_record_count -eq 0 ]]; then
            echo "#    no records for $table_name cnt=$table_record_count remove split file $file_name_split t_step=$t_step"
            rm $file_name_split
        fi
        table_record_count_downloaded=$(( $table_record_count_downloaded + $table_record_count ))
        t_start=$t_old  # move back in time
        t_old=$(( $t_start - $t_step ))
        if [[ $t_old -lt $(( $t_beginning * 100 )) ]]; then
            t_old=$(( $t_beginning * 100 ))
        fi
    done # reached t_beginning

    echo "## DONE downloading table cnt#=$table_record_count_downloaded == $table_record_count_expected now merge split files into $file_name.gz" | tee -a $download_path/_log.txt

    # jq fils all memory > 100G for large json files
    #cat $file_name.split.* | jq -s 'add' | gzip > $file_name.gz
    # https://www.shortcutfoo.com/app/dojos/awk/cheatsheet
    cat $file_name.split.* \
        | awk  'BEGIN{print "["}
                    /^\[/ && NR > 1 {print ","}
                    !/^[\[\]]\s*$/ {print}
                END{print "]"}
                ' \
        | gzip > $file_name.gz
    #
    rm $file_name.split.*
    echo "## Downloaded table: '$table_name' $table_record_count_downloaded records" | tee -a $download_path/_log.txt
    if [[ $table_record_count_downloaded -ne $table_record_count_expected ]]; then
        echo "##    Error $file_name wrong table_record_count_downloaded=$table_record_count_downloaded table_record_count_expected=$table_record_count_expected" | tee -a $download_path/_log.txt | tee -a $download_path/_error_query.txt
        # exit 1 . #For big tables split per day we dont have accurate count.
    fi
else

    echo "##    file already exists $file_name proceed to upload blob ..." | tee -a $download_path/_log.txt
    #table_record_count_downloaded="$(zcat ${file_name}.gz | jq '. | length' )"
    table_record_count_downloaded="$(zcat ${file_name}.gz | python3 ${0%/*}/json-count.py )"
fi

##
# Upload to blob if more than 1 record.
##
f="$file_name.gz"
if [[ $table_record_count_downloaded -eq 0 ]]; then
    echo "##    Skip upload record_count=$table_record_count_downloaded ${f}" | tee -a $download_path/_log.txt
else

    echo "##    Start upload to $container_name f=${f} cnt#=$table_record_count_downloaded == $table_record_count_expected" | tee -a $download_path/_log.txt
    if [[ "$container_sas_token" == "" ]]; then
        auth="--auth-mode login"
        echo "##    No container_sas_token set in config-${env}.sh using $auth"
    else
        auth="--sas-token $container_sas_token"
        echo "##    Using container_sas_token to auth to blob container $container_name"
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
        echo "##    ERROR with blob upload - exit"
        exit 1
    fi
    t="$(ElapsedMinutes ) minutes"
    echo "##    ${f##*/} uploaded to blob container. empty file ${t}" | tee -a $download_path/_log.txt
fi
rm ${f}
touch $file_name.uploadDone

#TheEnd.
