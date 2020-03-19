#!/bin/bash

g_timestamp=`date +%Y%m%d%H%M%S`

function extract_runs() {
   local infile=$1
   local outdir=$2
   if [ ! -e $outdir ]
   then
      mkdir $outdir
   fi
   local line i l1 l2 total
   local exeid sqlid
   local tmpexeid=/tmp/sqlexeid${g_timestamp}.txt
   local tmpfile=/tmp/sqlAEU${g_timestamp}.txt
   local tmprulefile=/tmp/sqlAEURule${g_timestamp}.txt
   local tmprulefile2=/tmp/sql2AEURule${g_timestamp}.txt
   grep "SparkListenerSQLAdaptiveExecutionUpdate" $infile|jq .executionId > $tmpexeid
   grep "SparkListenerSQLAdaptiveExecutionUpdate" $infile|jq .physicalPlanDescription |tr -d '"'> $tmpfile
   i=1
   total=`wc -l $tmpexeid|awk '{print $1}'`
   while [ $i -le $total ]
   do
     exeid=`sed -n ${i}p $tmpexeid`
     sqlid=`grep "SparkListenerSQLExecutionStart\",\"executionId\":$exeid," $infile|jq .description|tr -d '"'|sed 's/[ ]/|/g'|awk -F \| '{print $1}'`
     line=`sed -n ${i}p $tmpfile`
     echo "$line" |awk -F "Rules ===" '{print $2}' | sed 's/\\n/\n/g'|tr -s '\n'|awk '{print $1 "|" $2 "|" $4 "|" $5 "|" $7}' > $tmprulefile2
     l1=`wc -l $tmprulefile2|awk '{print $1-2}'`
     l2=$(($l1-4))
     head -n ${l1} $tmprulefile2|tail -n $l2 >$tmprulefile
     #echo "$line" |awk -F "Rules ===" '{print $2}' | sed 's/\\n/\n/g'|tr -s '\n'|awk '{print $1 "|" $2 "|" $4 "|" $5 "|" $7}'
     echo "Name|Value" > $outdir/effectivetime_${sqlid}.csv
     echo "Name|Value" > $outdir/totaltime_${sqlid}.csv
     echo "Name|Value" > $outdir/effectiverun_${sqlid}.csv
     echo "Name|Value" > $outdir/totalrun_${sqlid}.csv
     awk -F '|' '{print $1 "|" $2}' $tmprulefile >> $outdir/effectivetime_${sqlid}.csv
     awk -F '|' '{print $1 "|" $3}' $tmprulefile >> $outdir/totaltime_${sqlid}.csv
     awk -F '|' '{print $1 "|" $4}' $tmprulefile >> $outdir/effectiverun_${sqlid}.csv
     awk -F '|' '{print $1 "|" $5}' $tmprulefile >> $outdir/totalrun_${sqlid}.csv
     i=$(($i+1))
   done
   rm $tmpexeid $tmpfile $tmprulefile
}

if [ $# -ne 2 ]
then
   echo "Specify the <decompressed lz4 file> <out dir>"
   exit 1
fi

extract_runs $1 $2
