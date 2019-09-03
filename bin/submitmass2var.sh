#!/bin/sh
# $Id: submitmass2var.sh 15873 2013-10-29 16:47:24Z mgower $
# $Rev:: 15873                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-10-29 11:47:24 #$:  # Date of last commit.

# example of a shell-script which does mass production of DES jobs
# this may need lots of tweaks depending on what needs to be done.
#
# Assumes first var is xxxx and second var is yyyy
# input list is space separated 2 columns per submission  xxxx yyyy
#

if [ $# -lt 4 ]; then
  echo "Usage: submitmass2var.sh desfile inlist maxjobs site";
  exit 1;
fi
default=$1
inp=$2
maxjobs=$3
site=$4

# for each line in the input list
while read line;  do
    firstvar=`echo $line | awk '{print $1}'`;
    secondvar=`echo $line | awk '{print $2}'`;

    dbase=`basename $default`;
    outfile=`echo $dbase | sed -e "s/xxxx/$firstvar/ig"`;

    # skip already existing submit files
    if [ -f $outfile ]; then
        echo "Skipping $outfile because previously submitted ($line)";
    else
        runs=`desstat|grep $USER| grep $site | wc -l `;
        while [ $runs -ge $maxjobs ]; do
            sleep 30;
            runs=`desstat|grep $USER| grep $site | wc -l `;
        done;

        sed -e "s/xxxx/$firstvar/ig" $default | sed -e "s/yyyy/$secondvar/ig" > $outfile;

        #now submit the file
        echo "Submitting" $outfile;
        dessubmit $outfile
        sleep 30
    fi; 
done < $inp
