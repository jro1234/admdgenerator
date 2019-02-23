#!/bin/bash


WORKERID=$ALPS_APP_PE
export ADMD_DBURL=$2
NWORKERS=$3
echo "adaptivemdworker $1 &> $1.worker.$WORKERID.log"

export OMP_NUM_THREADS=16

sleep $(echo `printf "0.%03d\n" $(( RANDOM % 1000 ))`*0.8*$NWORKERS | bc)

adaptivemdworker $1 -v &> adaptivemd.worker.$WORKERID.log
