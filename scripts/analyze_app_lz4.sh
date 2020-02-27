#!/bin/bash

g_current_dir=$(dirname $(readlink -f $0))

if [ $# -ne 1 ]
then
  echo "Specify *.lz4"
  exit 1
fi

lz4decompressor=$g_current_dir/../../InMemSparkLZ4Decompressor/target/InMemSparkLZ4Decompressor-1.0-SNAPSHOT-jar-with-dependencies.jar

if [ ! -e $lz4decompressor ];then
  echo "Please build $lz4decompressor"
  exit 1
fi

input=$1
java -jar $lz4decompressor $input
fname=`echo "$input"| cut -d'.' -f1`
#java -Dspark.master=local -jar SparkEventLogAnalytic/target/SparkEventLogAnalytic-1.0-SNAPSHOT-jar-with-dependencies.jar -i ${fname}.txt -o /tmp -s -t
timestamp=`date +%Y%m%m%H%M%S`
metrics_outdir=/tmp/mysparkevent-$timestamp
sh ${g_current_dir}/analyze_app_log.sh ${fname}.txt $metrics_outdir
sh ${g_current_dir}/post_process_csv.sh $metrics_outdir
