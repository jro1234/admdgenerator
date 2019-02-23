#!/bin/bash


DBPATH=$1
DBPORT=$2
LAUNCH=$3

if [ -z "$DBPORT" ]
then
  DBPORT=27017
fi

if [ -z "$DBPATH" ]
then
  echo "Need to give a name for mongodb instance as script argument"
  exit 1
else
  if ! [ -z "`lsof -Pi :$DBPORT | grep $DBPORT`" ]
  then
    echo "Port $DBPORT is already in use, try giving a different port as second argument"
    exit 1
  #else
  #  echo "Starting mongod instance with database in folder: $DBPATH/db"
  fi
fi

mkdir -p $DBPATH
mkdir $DBPATH/db
mkdir $DBPATH/socket

echo -e "net:\n   unixDomainSocket:\n      pathPrefix: $DBPATH/socket\n   bindIp: 0.0.0.0\n   port:   $DBPORT\n" > $DBPATH/db.cfg

# On OLCF Titan
#echo "numactl --interleave=all mongod --dbpath $DBPATH/db/ --config $DBPATH/db.cfg > $DBPATH/db.log"
if [ "$LAUNCH" = "--launch" ]
then
  echo "got args: $DBPATH $DBPORT $LAUNCH"
  echo "no backgrounding"
  numactl --interleave=all mongod --dbpath $DBPATH/db/ --config $DBPATH/db.cfg &> $DBPATH/db.log
  echo $!
  date +%s
else
  numactl --interleave=all mongod --dbpath $DBPATH/db/ --config $DBPATH/db.cfg > $DBPATH/db.log & MPID=$!
  #export MPID
  echo $MPID
  #mongod --dbpath $DBPATH/db/ --config $DBPATH/db.cfg > $DBPATH/db.log & MPID=$!
  #echo "Use 'MPID' as variable for mongod process with ID $MPID"
fi
