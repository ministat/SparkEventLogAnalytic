#!/bin/bash

function pandas_merge() {
  local left=$1
  local right=$2
  local out=$3
  local timestamp=`date +%Y%m%m%H%M%S`
  local tmp_py=/tmp/merge${timestamp}.py
cat << EOF > $tmp_py
import pandas as pd
df1 = pd.read_csv("$left", sep='|')
df2 = pd.read_csv("$right", sep='|')
outfile = pd.merge(df1, df2, how='outer', left_on='Name', right_on='Name')
outfile.to_csv("$out", sep='|', index=False)
EOF
  python $tmp_py
  rm $tmp_py
}

function merge_stage_metrics() {
  local input_dir=$1
  local timestamp=`date +%Y%m%m%H%M%S`
  local name column_name="Name"
  local i c=0
  local final_out_with_hdr=stage_metrics.csv
  local final_out=stage_metrics_no_hdr.csv
  local tmp_out=/tmp/stage${timestamp}.txt
  local tmp_final=/tmp/final${timestamp}.csv
  for i in `find $input_dir -path "*stage*/*.csv" -type f`
  do
    # from /tmp/sparkeventlog-20200202124805/execId252-sqlq8_ddb50b47-02d1-/stagesingle_eventlog/part-00000-4fcc9265-2c2b-414e-ba66-8a762946fed5-c000.csv
    # extract 'sqlq8'
    name=`echo "$i"|awk -F \/ '{print $4}'|awk -F - '{print $2}'|awk -F _ '{print $1}'`
    column_name="${column_name}|${name}"
    #if [ $c -eq 0 ];then
    #  awk -F \| '{print $1}' $i > $final_out
    #fi
    #awk -v a=$name -F \| '{if ($0 ~ "value") print a; else print $2}' $i > $tmp_out
    #paste -d \| $final_out $tmp_out > $tmp_final
    if [ $c -eq 0 ];then
      cp $i $final_out
    else
      pandas_merge $final_out $i $tmp_final
      rm $final_out
      mv $tmp_final $final_out
    fi
    c=$(($c+1))
  done
  echo "$column_name" > $final_out_with_hdr
  sed -i '1d' $final_out
  cat $final_out >> $final_out_with_hdr
  rm $tmp_final
}

if [ $# -ne 1 ];then
  echo "Specify dir"
  exit 1
fi
merge_stage_metrics $1
