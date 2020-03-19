#!/bin/bash

if [ $# -ne 1 ];
then
   echo "Specify the indir"
   exit 1
fi

indir=$1
for i in `find $indir -iname "*--bbensid*.csv" -type f`
do
   a=`grep -n "^|" $i`
   if [ "$a" != "" ];
   then
      l=`echo "$a"|awk -F : '{print $1}'`
      l=$(($l-1))
      head -n $l $i > b
      n=`basename $i`
      rm $i
      mv b $indir/$n
   fi
done
