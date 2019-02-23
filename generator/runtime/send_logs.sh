#!/bin/bash


function latest {
  if [ "$(ls -A sessions)" ]
  then
    LATEST=$(for d in sessions/*; do echo -n "$d"; echo; done | sort -V | tail -n1)
  else
    LATEST=""
  fi
  #unset -v LATEST
  #for file in logs/*
  #do
  #  [[ $file -nt $LATEST ]] && LATEST=$file
  #done
  echo $LATEST
  return 0
}


# Moving files around, first need to check then number
# of iterations we have already done. 
# We will move the previous final job files there
# before we start doing other stuff in this job since
# these aren't moved at the end of a job.
max=0

latestdir=`latest`
if [ -z $latestdir ]
then
  echo "This seems to be the first workload"
  max=0001
  latestdir="sessions/admd-$max.`date +"%Y-%m-%d"`"
  mkdir $latestdir
else
  max=$(echo $latestdir | awk -F"." '{print $1}' | tr -dc '0-9')
fi
echo "Last round logs going to this folder: $latestdir"
# move logs created during the last workload
mv rp.session* *out *err *log $latestdir


next=$((10#$max+1))
if (( $next < 1000 ))
then
  if (( $next < 100 ))
  then
    if (( $next < 10 ))
    then
      next=000$next
    else
      next=00$next
    fi
  else
    next=0$next
  fi
fi

nextround="sessions/admd-$next.`date +"%Y-%m-%d"`"
echo "Next Round Folder Name: $nextround"
mkdir $nextround
