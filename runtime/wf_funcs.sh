#-----------------------------------------------#
#  Shell functions used during the execution    #
#  of AdaptiveMD workflows. These are used by   #
#  multiple layers of our execution scheme      #
#-----------------------------------------------#

function checkpoint {
  echo "Did you prepare margs?"
  echo "How about correct database is live and connected if using RP?"
  echo "Typed in your workflow setup correctly?"
  read -t 1 -n 999999 discard 
  read -n 1 -p  " >>> Type \"y\" if ready to proceed: " proceedinput
  if [ "$proceedinput" = "y" ]
  then
    echo ""
    echo "Moving to next phase"
    return 0
  else
    exit 0
  fi
}


function waitfor {
  jobstate="NONE"
  echo "Waiting for AdaptiveMD workload in PBS job #$1"
  if [ "$#" = "2" ]
  then
    if [ -f "$2" ]
    then
      echo "Looking in file $2 for job state updates"
    else
      echo "Error: Cannot find the given job state file $2"
      exit 1
      #return
    fi
  fi
  while [ "$jobstate" != "C" ]
  do
    if [ "$#" = "2" ]
    then
      jobstate=$(cat $2)
    else
      jobstate=$(qstat | grep $1 | awk '{for(i=1;i<NF+1;i++){if($i=="Q"||$i=="R"||$i=="C"){print $i}}}')
    fi
    sleep 5
  done
  echo "AdaptiveMD workload in PBS job #$1 has finished"
  return 0
}


function rescue_tasks {
  echo "Using AdaptiveMDWorkers to do a Rescue Workload"
  echo "Rescue Workload not yet configured for RP"
  echo "Got $# arguments: "
  echo $@
  NTRIED=$1
  NTRIES=$2
  if [[ "$NTRIED" -ge "$NTRIES" ]]
  then
    echo "Already made $NTRIED attempts to rescue tasks, rescue failed"
    exit 1
  fi
  echo "Rescue Workload $NTRIED of $NTRIES attempts to make"
  NNODES=$(($6+1))
  echo "Using $NNODES nodes to execute $5 tasks"
  DLM="-"
  if [[ "$5" == *"$DLM"* ]]
  then
    RSC=$(echo $5 | awk -F"$DLM" '{print $1}')
    TARGETQ="-q $RSC"
    WKLTYPE=$(echo $5 | awk -F"$DLM" '{print $2}')
  else
    RSC=""
    TARGETQ="-V"
    WKLTYPE="$5"
  fi
  echo "'$5': recovering tasks for resource '$TARGETQ', for a workload type $WKLTYPE"
  DLM=":"
  DBHOME=$(echo ${13} | awk -F"$DLM" '{print $1}')
  DBPORT=$(echo ${13} | awk -F"$DLM" '{print $2}')
  if [ -z "$DBPORT" ]
  then
    DBPORT="27017"
  fi
  #NETDEVICE from bashrc
  #NETDEVICE="bond0"
  LOGIN_HOSTNAME=`ip addr show $NETDEVICE | grep -Eo '(addr:)?([0-9]*\.){3}[0-9]*' | head -n1`
  ADMD_DBURL="mongodb://$LOGIN_HOSTNAME:$DBPORT/"
  if [ ! -z $RSC ]
  then
    JOBSTATEFILE="admd.$RSC.state"
    echo "Q" > $JOBSTATEFILE
  else
    JOBSTATEFILE=""
  fi
  #submitcommand="qsub $TARGETQ -N $2.$1.$WKLTYPE.admd -l nodes=$NNODES -l walltime=0:${10}:0 -F $1*$2*cleanup*$4*$5*$6*$7*$8*$9*${10}*$DBHOME*$JOBSTATEFILE $ADMD_RUNTIME/exectasks.pbs"
  #submitcommand="qsub         -N $4.$3.rescue.admd -l nodes=$NNODES -l walltime=0:${12}:0 -F $3*$4*cleanup*$6*$7*$8*$9*${10}*${11}*${12}*$DBHOME*$JOBSTATEFILE $ADMD_RUNTIME/exectasks.pbs"
  submitcommand="qsub $TARGETQ -N $4.$3.rescue.admd -l nodes=$NNODES -l walltime=0:${12}:0 -F $3*$4*cleanup*$6*$7*$8*$9*${10}*${11}*${12}*$DBHOME*$JOBSTATEFILE $ADMD_RUNTIME/exectasks.pbs"
  echo $submitcommand
  ADMD_JOBID=$(eval $submitcommand)
  echo "Initiated AdaptiveMD workload in PBS job# $ADMD_JOBID"
  waitfor $ADMD_JOBID $JOBSTATEFILE
  #wait $APP_PID
  JOB_STATUS="$?"
  rm $JOBSTATEFILE
  echo "Starting database at $DBHOME on port $DBPORT"
  MPID=$($ADMD_RUNTIME/launch_amongod.sh $DBHOME $DBPORT)
  sleep 10
  echo "Mongo DB process ID: $MPID"
  echo "Running AdaptiveMD Application to check for task rescue after rescue:"
  appcommand="$ADMD_RUNTIME/application.sh $3 $4 $5 0 $7 $8 $9 ${10} ${11} ${12} --rescue_only"
  APP_OUT=$(eval $appcommand)
  IFS=$'\n' APP_OUT=($APP_OUT)
  #APP_STATUS="${APP_OUT[${#APP_OUT[@]}-1]}"
  #APP_STATUS="$?"
  APP_STATUS="${APP_OUT[${#APP_OUT[@]}-1]}"
  echo "Got Status '$APP_STATUS' from AdaptiveMD after rescue try"
  echo "killing Mongo DB"
  kill $MPID
  wait $MPID
  sleep 5
  if [ "$(ls -A $DBHOME/socket)" ]
  then
    rm $DBHOME/socket/*
    rm $DBHOME/db/mongod.lock
  fi
  if [[ $APP_STATUS =~ ^[-+]?([1-9][[:digit:]]*|0)$ ]]
  then
      if [[ $APP_STATUS -lt 0 ]]
      then
        echo "Exiting, AdaptiveMD application error"
        exit 1
      elif [[ $APP_STATUS -eq 0 ]]
      then
        echo "No incomplete/failed tasks, rescue was successful"
        return
      elif [[ $APP_STATUS -gt 0 ]]
      then
        echo "Going to rescue incomplete/failed tasks"
        echo "Moving output logs from last rescue try"
        bash $ADMD_RUNTIME/send_logs.sh
        rescue_tasks $((NTRIED+1)) $2 $3 $4 $5 $APP_STATUS $7 $8 $9 ${10} ${11} ${12} ${13}
      else
        echo "This post-closed condition should not appear"
      fi
  else
      echo "Exiting, $APP_STATUS not processable as int"
      exit 1
  fi
}


function exec_workload {
  # args are:
  #       1          2          3     4       5        6        7
  # roundnumber projname wkloadtype ntraj mdsteps tjsteps afterntjs
  #           8          9       10      11      12     13   14      15
  # anlyztjlength samplefunc minutes execflag sysname mfreq pgreq platform
  echo "Got these arguments for workload:"
  echo $@
  if [[ ${10} =~ ^[0-9]+$ ]]
  then
    echo "executing for ${10} minutes"
      if  [ "${11}" = "--rp" ]
      then
        echo "Using Radical Pilot to Execute Workload"
        $ADMD_RUNTIME/application.sh $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14} ${15}
        APP_STATUS="$?"

      else
        echo "Using AdaptiveMDWorkers to Execute Workload"
        NNODES=$(($4+1))
        echo "Using $NNODES nodes to execute $4 tasks"
        # On Titan, can't tunnel out from compute nodes
        # so its more complicated ...
        #  - The MongoDB is going to be taken up and
        #    down inside and outside of compute jobs
        #  - Workload 1.1 - submit tasks outside of PBS job
        #                    - could involve PCCA calculation
        #                      if using explore_macrostates
        #  - Workload 1.2 - execute using 'cleanup' option in PBS job
        # TODO split ${11} ie execflag on ":" to see if
        #      a non-default port is specified
        # FIXME this delimiter is valid URL character
        DLM="-"
        if [[ "$3" == *"$DLM"* ]]
        then
          RSC=$(echo $3 | awk -F"$DLM" '{print $1}')
          TARGETQ="-q $RSC"
          WKLTYPE=$(echo $3 | awk -F"$DLM" '{print $2}')
        else
          RSC=""
          TARGETQ="-V"
          WKLTYPE="$3"
        fi
        echo "$3 gave: $TARGETQ, $WKLTYPE"
        DLM=":"
        DBHOME=$(echo ${11} | awk -F"$DLM" '{print $1}')
        DBPORT=$(echo ${11} | awk -F"$DLM" '{print $2}')
        if [ -z "$DBPORT" ]
        then
          DBPORT="27017"
        fi
        #NETDEVICE from bashrc
        #NETDEVICE="bond0"
        LOGIN_HOSTNAME=`ip addr show $NETDEVICE | grep -Eo '(addr:)?([0-9]*\.){3}[0-9]*' | head -n1`
        ADMD_DBURL="mongodb://$LOGIN_HOSTNAME:$DBPORT/"
        echo "Starting database at $DBHOME on port $DBPORT"
        MPID=$($ADMD_RUNTIME/launch_amongod.sh $DBHOME $DBPORT)
        sleep 10
        echo "Mongo DB process ID: $MPID"
        echo "Running AdaptiveMD Application:"
        appcommand="$ADMD_RUNTIME/application.sh $1 $2 $WKLTYPE $4 $5 $6 $7 $8 $9 ${10} --submit_only ${12} ${13} ${14} ${15}"
        APP_OUT=$(eval $appcommand)
        IFS=$'\n' APP_OUT=($APP_OUT)
        #APP_STATUS="${APP_OUT[${#APP_OUT[@]}-1]}"
        #APP_STATUS="$?"
        APP_STATUS="${APP_OUT[${#APP_OUT[@]}-1]}"
        echo "Got Status '$APP_STATUS' from AdaptiveMD"
        echo "killing Mongo DB"
        kill $MPID
        wait $MPID
        sleep 5
        if [ "$(ls -A $DBHOME/socket)" ]
        then
          rm $DBHOME/socket/*
          rm $DBHOME/db/mongod.lock
        fi
        if [[ $APP_STATUS =~ ^[-+]?([1-9][[:digit:]]*|0)$ ]]
        then
            if [[ $APP_STATUS -lt 0 ]]
            then
              echo "Exiting, AdaptiveMD application error"
              exit 1
            elif [[ $APP_STATUS -eq 0 ]]
            then
              echo "No incomplete/failed tasks, executing given workload"
            elif [[ $APP_STATUS -gt 0 ]]
            then
              echo "Going to rescue incomplete/failed tasks"
              NTRIES=3
              rescue_tasks "0" $NTRIES $1 $2 $3 $APP_STATUS $5 $6 $7 $8 $9 ${10} ${11}
            else
              echo "This post-closed condition should not appear"
            fi
        else
            echo "Exiting, $APP_STATUS not castable as int"
            exit 1
        fi
        if [ ! -z $RSC ]
        then
          JOBSTATEFILE="admd.$RSC.state"
          echo "Q" > $JOBSTATEFILE
        else
          JOBSTATEFILE=""
        fi

        submitcommand="qsub $TARGETQ -N $2.$1.$WKLTYPE.admd -l nodes=$NNODES -l walltime=0:${10}:0 -F $1*$2*cleanup*$4*$5*$6*$7*$8*$9*${10}*$DBHOME*$JOBSTATEFILE $ADMD_RUNTIME/exectasks.pbs"
        echo $submitcommand
        ADMD_JOBID=$(eval $submitcommand)
        # TODO add check of submission (maybe just if [ -z $ADMD_JOBID ])
        echo "Initiated AdaptiveMD workload in PBS job# $ADMD_JOBID"
        waitfor $ADMD_JOBID $JOBSTATEFILE
        #wait $APP_PID
        JOB_STATUS="$?"
        rm $JOBSTATEFILE

        echo "Starting database at $DBHOME on port $DBPORT"
        MPID=$($ADMD_RUNTIME/launch_amongod.sh $DBHOME $DBPORT)
        sleep 10
        echo "Mongo DB process ID: $MPID"
        echo "Running AdaptiveMD Application to check for task rescue from this workload:"
        appcommand="$ADMD_RUNTIME/application.sh $1 $2 $WKLTYPE 0 $5 $6 $7 $8 $9 ${10} --rescue_only"
        echo $appcommand
        APP_OUT=$(eval $appcommand)
        IFS=$'\n' APP_OUT=($APP_OUT)
        #APP_STATUS="${APP_OUT[${#APP_OUT[@]}-1]}"
        #APP_STATUS="$?"
        APP_STATUS="${APP_OUT[${#APP_OUT[@]}-1]}"
        echo "Got Status '$APP_STATUS' from AdaptiveMD after checking for completion of last workload"
        echo "killing Mongo DB"
        kill $MPID
        wait $MPID
        sleep 5
        if [ "$(ls -A $DBHOME/socket)" ]
        then
          # FIXME there's a safer solution
          rm $DBHOME/socket/*
          rm $DBHOME/db/mongod.lock
        fi
        if [[ $APP_STATUS =~ ^[-+]?([1-9][[:digit:]]*|0)$ ]]
        then
            if [[ $APP_STATUS -lt 0 ]]
            then
              echo "Exiting, AdaptiveMD application error"
              exit 1
            elif [[ $APP_STATUS -eq 0 ]]
            then
              echo "No incomplete/failed tasks, workload completed successfully"
            elif [[ $APP_STATUS -gt 0 ]]
            then
              echo "Going to rescue incomplete/failed tasks"
              NTRIES=3
              rescue_tasks "0" $NTRIES $1 $2 $3 $APP_STATUS $5 $6 $7 $8 $9 ${10} ${11}
            else
              echo "This post-closed condition should not appear"
            fi
        else
            echo "Exiting, $APP_STATUS not castable as int"
            exit 1
        fi
      fi
  fi

  echo "AdaptiveMD Job Exit Status: $JOB_STATUS"
  if [ "$JOB_STATUS" != "0" ]
  then
    echo "Exiting, got Error from job status: $JOB_STATUS"
    exit 1
  fi

  echo "Moving output logs from last workload"
  #sh $ADMD_RUNTIME/send_logs.sh $(latest)
  bash $ADMD_RUNTIME/send_logs.sh
  echo "Workload is complete"
}


