#!/bin/bash

function pandas_merge() {
   local left=$1
   local right=$2
   local out=$3
   local column=$4
   local timestamp=`date +%Y%m%d%H%M%S`
   local tmp_py=/tmp/mrg${timestamp}.py
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

function merge_csv() {
   local input_dir=$1
   local final_out_with_hdr=$2
   local prefix=$3
   local timestamp=`date +%Y%m%d%H%M%S`
   local final_out=/tmp/sqlrulefinal${timestamp}.csv
   local tmp_out=/tmp/sqlt${timestamp}.csv
   local i c=0
   local column="Name"
   local column_name="Name"
   local name
   local call=pandas_merge
   for i in `find $input_dir -iname "${prefix}*" -type f`
   do
      name=`echo "$i"|sed 's/.*bbensid_\([a-zA-Z0-9]*\).*/\1/g'`
      column_name="${column_name}|${name}"
      if [ $c -eq 0 ];then
	 #echo $i
	 cp $i $final_out
      else
	 if [ -e $final_out ];then
            echo "$final_out $i $tmp_out $column"
	    $call $final_out $i $tmp_out $column
	    rm $final_out
	    mv $tmp_out $final_out
	    #cat $final_out
	 else
	    echo "run iter: $c"
	    break
         fi
      fi
      c=$(($c+1))
   done
   echo "$column_name" > $final_out_with_hdr
   sed -i '1d' $final_out
   cat $final_out >> $final_out_with_hdr
   #rm $final_out
}

if [ $# -ne 1 ]
then
   echo "Specify the input dir which contains raw csv"
   exit 1
fi

indir=$1
#merge_csv $indir effective_time.csv effectivetime_--bbensid
merge_csv $indir effective_run.csv effectiverun_--bbensid
merge_csv $indir total_time.csv totaltime_--bbensid
merge_csv $indir total_run.csv totalrun_--bbensid

