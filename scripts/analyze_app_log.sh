#!/bin/bash

g_current_dir=$(dirname $(readlink -f $0))

function extract_first_sql_execution() {
  local input_file=$1
  local output_file=$2

  awk '/\{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"/{a=1};a;/\{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd"/{exit}' $input_file > $output_file
}

function remove_first_matched_execution() {
  local input_file=$1
  local output_file=$2
  awk -f $g_current_dir/awk_other_sql_execution_than_1st_script $input_file > $output_file
}

function generate_execution_name() {
  local input_file=$1
  local execution_id desc
  execution_id=`grep "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart" $input_file|jq .executionId`
  desc=`grep "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart" $input_file|jq .description|tr -d '"'|sed 's/[()*\/: ]//g'|cut -c1-20`
  echo "execId${execution_id}-$desc"
}

function main_log_parser() {
  local input_file=$1
  local timestamp=`date +%Y%m%m%H%M%S`
  local jar_file=${g_current_dir}/../target/SparkEventLogAnalytic-1.0-SNAPSHOT-jar-with-dependencies.jar
  local prefix=sparkeventlog
  local tmp_dir=/tmp/${prefix}-${timestamp}
  local tmp_execute_log=$tmp_dir/single_eventlog.txt
  local tmp_follow_log=$tmp_dir/follow_eventlog.txt
  local tmp_input=$tmp_dir/tmp_input.txt
  if [ ! -d $tmp_dir ];
  then
    mkdir $tmp_dir
  fi

  while [ 1 ]; do
    extract_first_sql_execution $input_file $tmp_execute_log
    if [ ! -s $tmp_execute_log ]; then
      # no execution was found in the log file
      break
    fi
    local name=$(generate_execution_name $tmp_execute_log)
    java -Dspark.master=local -jar $jar_file -i $tmp_execute_log -o $tmp_dir/$name -t -s
    remove_first_matched_execution $input_file $tmp_follow_log
    if [ -e $tmp_input ]; then
      rm $tmp_input
    fi
    cp $tmp_follow_log $tmp_input
    input_file=$tmp_input
  done
}

if [ $# -ne 1 ]
then
   echo "Specify the <event log file>"
   exit 1
fi

input=$1
main_log_parser $input
