#!/bin/bash

function pandas_transpose() {
  local in=$1
  local out=$2
  local timestamp=`date +%Y%m%m%H%M%S`
  local tmp_py=/tmp/transpose${timestamp}.py
cat << EOF > $tmp_py
import pandas as pd
df1 = pd.read_csv("$in").transpose()
outfile.to_csv("$out", index=False)
EOF
  python $tmp_py
  rm $tmp_py
}

function pandas_merge() {
  local left=$1
  local right=$2
  local out=$3
  local column=$4
  local timestamp=`date +%Y%m%m%H%M%S`
  local tmp_py=/tmp/merge${timestamp}.py
cat << EOF > $tmp_py
import pandas as pd
df1 = pd.read_csv("$left", sep='|')
df2 = pd.read_csv("$right", sep='|')
outfile = pd.merge(df1, df2, how='outer', left_on="$column", right_on="$column")
outfile.to_csv("$out", sep='|', index=False)
EOF
  python $tmp_py
  rm $tmp_py
}

function merge_stage_metrics() {
  local input_dir=$1
  local final_out_with_hdr=$2
  local handle_elapse=$3
  local timestamp=`date +%Y%m%m%H%M%S`
  local name column_name="Name"
  local i c=0
  local final_out=stage_metrics_no_hdr.csv
  local tmp_out=/tmp/stage${timestamp}.txt
  local tmp_final=/tmp/final${timestamp}.csv
  local call=pandas_merge
  local column="Name"
  local csvpat="*stage*/*.csv"
  if [ "$handle_elapse" == "1" ];then
    column="elapse"
    csvpat="*elapse*/*.csv"
  fi
  for i in `find $input_dir -path "$csvpat" -type f`
  do
    # from /tmp/sparkeventlog-20200202124805/execId252-sqlq8_ddb50b47-02d1-/stagesingle_eventlog/part-00000-4fcc9265-2c2b-414e-ba66-8a762946fed5-c000.csv
    # extract 'sqlq8'
    name=`echo "$i"|awk -F \/ '{print $4}'|awk -F - '{print $1"-"$2}'|awk -F _ '{print $1}'`
    column_name="${column_name}|${name}"
    if [ $c -eq 0 ];then
      cp $i $final_out
    else
      $call $final_out $i $tmp_final $column
      rm $final_out
      mv $tmp_final $final_out
    fi
    c=$(($c+1))
  done
  echo "$column_name" > $final_out_with_hdr
  if [ "$need_transpose" == "1" ];then
    pandas_transpose $final_out $tmp_final
    rm $final_out
    mv $tmp_final $final_out
  fi
  sed -i '1d' $final_out
  cat $final_out >> $final_out_with_hdr
  rm $tmp_final
  rm $final_out
}

if [ $# -lt 1 ];then
  echo "Specify <input_dir> (<output_csv>)"
  echo "If output_csv is not specified, stage_metrics.csv is the default output"
  exit 1
fi
function usage() {
cat << EOF
 $0:<options>
    -h             print help
    -i <inputdir>  Required. Specify the input dir contains all csv
    -o <out.csv>   Optional. Specify the output csv file. Default is stage_metrics.csv
    -t             Optional. Whether you want to parse elapse time. Default is not
EOF
 exit 1
}

input_dir=$1
output=stage_metrics.csv
need_transpose=0
while getopts 'hi:o:t' c
do
  case $c in
   i) input_dir=$OPTARG;;
   o) output=$OPTARG;;
   h) usage;;
   t) need_transpose=1;;
  esac
done

merge_stage_metrics $input_dir $output $need_transpose
