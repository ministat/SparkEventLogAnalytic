#!/bin/bash

g_timestamp=`date +%Y%m%d%H%M%S`

function extract_runs() {
   local infile=$1
   local outdir=$2
   if [ ! -e $outdir ]
   then
      mkdir $outdir
   fi
   local line i
   local exeid
   local tmpexeid=/tmp/sqlexeid${g_timestamp}.txt
   local tmpfile=/tmp/sqlAEU${g_timestamp}.txt
   local tmprulefile=/tmp/sqlAEURule${g_timestamp}.txt
   grep "SparkListenerSQLAdaptiveExecutionUpdate" $infile|jq .executionId > $tmpexeid
   grep "SparkListenerSQLAdaptiveExecutionUpdate" $infile|jq .physicalPlanDescription |tr -d '"'> $tmpfile
   i=1
   while read line
   do
     exeid=`sed -n ${i}p $tmpexdid`
     echo "$line" |awk -F "Rules ===" '{print $2}' | sed  's/\\n/\n/g'|tr -s '\n'|awk '{print $1 "|" $2 "|" $4 "|" $5 "|" $7}' > $tmprulefile
     echo "Name|Value" > $outdir/effectivetime_${exeid}.csv
     echo "Name|Value" > $outdir/totaltime_${exeid}.csv
     echo "Name|Value" > $outdir/effectiverun_${exeid}.csv
     echo "Name|Value" > $outdir/totalrun_${exeid}.csv
     awk -F '|' '{print $1 "|" $2}' $tmprulefile >> $outdir/effectivetime_${exeid}.csv
     awk -F '|' '{print $1 "|" $3}' $tmprulefile >> $outdir/totaltime_${exeid}.csv
     awk -F '|' '{print $1 "|" $4}' $tmprulefile >> $outdir/effectiverun_${exeid}.csv
     awk -F '|' '{print $1 "|" $5}' $tmprulefile >> $outdir/totalrun_${exeid}.csv
     i=$(($i+1))
   done < $tmpfile
   awk -F "Rules ===" '{print $2}' | sed  's/\\n/\n/g'
}

if [ $# -ne 2 ]
then
   echo "Specify the <decompressed lz4 file> <out dir>"
   exit 1
fi

extract_runs $1 $2
