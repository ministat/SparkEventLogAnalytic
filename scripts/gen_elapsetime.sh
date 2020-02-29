#!/bin/bash

g_current_dir=$(dirname $(readlink -f $0))
g_timestamp=`date +%Y%m%d%H%M%S`
g_tmp_dir=/tmp/sparkevent-elaspe-${g_timestamp}

function pandas_transpose() {
  local in=$1
  local out=$2
  local timestamp=`date +%Y%m%d%H%M%S`
  local tmp_py=/tmp/transpose${timestamp}.py
cat << EOF > $tmp_py
import pandas as pd
df1 = pd.read_csv("$in").transpose()
df1.to_csv("$out", index=False)
EOF
  python $tmp_py
  rm $tmp_py
}

function pandas_merge() {
  local left=$1
  local right=$2
  local out=$3
  local column=$4
  local timestamp=`date +%Y%m%d%H%M%S`
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
function analyze_dir() {
  local indir=$1
  local outputdir=$2
  local i j c=0
  local name
  local timestamp=`date +%Y%m%d%H%M%S`
  local final_out=stage_metrics_no_hdr.csv
  local tmp_out=/tmp/elapse${timestamp}.txt
  local tmp_final=/tmp/final${timestamp}.csv
  local call=pandas_merge
  local final_out_with_hdr=$outputdir/final_elapse.csv
  local column="elapse" column_name="Name"
  local jar_file=${g_current_dir}/../target/SparkEventLogAnalytic-1.0-SNAPSHOT-jar-with-dependencies.jar
  if [ ! -e $jar_file ];
  then
     cd ${g_current_dir}/../
     mvn clean package
     cd -
  fi
  for i in `ls ${indir}/single_event*execId*`
  do
    local f=`grep "Stage Info" $i|head -n1`
    if [ "$f" == "" ];then
       echo "no stage metrics: $i"
       continue
    fi
    j=`basename $i`
    name=`echo $j|awk -F . '{print $1}'|awk -F _ '{print $3}'`
    column_name="${column_name}|${name}"
    java -Dspark.master=local -jar $jar_file -i $i -o $outputdir/${name} -e
    j=`find $outputdir/${name} -iname "*.csv"`
    if [ "$j" == "" ];then
       continue
    fi
    if [ $c -eq 0 ];then
      cp $j $final_out
    else
      $call $final_out $j $tmp_final $column
      rm $final_out
      mv $tmp_final $final_out
    fi
    c=$(($c+1))
  done
  echo "$column_name" > $final_out_with_hdr
  pandas_transpose $final_out $tmp_final
  rm $final_out
  mv $tmp_final $final_out
  sed -i '1d' $final_out
  cat $final_out >> $final_out_with_hdr
  rm $final_out
}

if [ $# -lt 1 ];then
  echo "Specify the <inputdir> (<outdir>)"
  exit 1
fi

inputdir=$1
outdir=$g_tmp_dir
if [ $# -eq 2 ];then
  outdir=$2
fi
analyze_dir $inputdir $outdir
