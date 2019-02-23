#!/bin/bash

# Use to delete and restart when testing
# WARNING!!!
# -->> VERY DANGEROUS, DONT FORGET TO UNDO
#      ONCE YOU WANT TO KEEP DATA
#REINIT="--reinit"
REINIT=

echo "\$ADMD_RUNTIME/application.sh got args:"
echo "$@"

# used for TITAN Exec Config
OPENMM_CPU_THREADS=$OMP_NUM_THREADS
MINUTES=${10}
EXECFLAG=${11}

# System Identification
PROJNAME=$2
SYSTEMNAME=${12}

# Workload Control
BATCHSLEEP=5
BATCHSIZE=5000
if [ "$EXECFLAG" = "--submit_only" ]
then
  BATCHWAIT=
else
  BATCHWAIT="-u all"
fi

# Workflow Parameters
ROUND_N=$1
WORKLOAD=$3
if [[ $WORKLOAD = "cleanup" ]]
then
  RESCUE_TASKS=
else
  RESCUE_TASKS="--rescue_tasks"
fi
PROGRESS=any
NTRAJ=$4
NSTEPS=$5
TSTEPS=$6
AFTERNTRAJS=$7
MODELTRAJLENGTH=$8
SAMPLEFUNC=$9

# Simulation Data Parameters
MFREQ=${13}
PFREQ=${14}
PLATFORM=${15}
# If you have a system set up for 5fs
# timesteps use this parameter (recommended)
#  --> FIXME steps are given in 2fs unit
#            and converted to 5fs equivalent
LONGTS="--longts"
#LONGTS=


SCRIPT_NAME=run_admd.py
ADMD_SCRIPT=$ADMD_GENERATOR/scripts/$SCRIPT_NAME

# Initialize the run
if [ ! -z "$SYSTEMNAME" ]
then
  python $ADMD_SCRIPT $PROJNAME $SYSTEMNAME $REINIT --init_only -P $PLATFORM -p $PFREQ -m $MFREQ
fi


#--------------------------#
#---   Execute the run  ---#
#--------------------------#

# CLEANUP
if [[ $WORKLOAD = "cleanup" ]]; then
  echo -e "\n CLEANUP PREVIOUS TASKS"
  echo "python $ADMD_SCRIPT $PROJNAME -R $ROUND_N -s $BATCHSLEEP -c $BATCHSIZE $BATCHWAIT --progress $PROGRESS -w $ADMD_ENV_ACTIVATE  $LONGTS -t $OPENMM_CPU_THREADS -l $NSTEPS -k $TSTEPS -N $NTRAJ -b 1 -x 1 --minutes $MINUTES $EXECFLAG $RESCUE_TASKS"
  echo -e "\n\n"
  exitval=$(python $ADMD_SCRIPT $PROJNAME -R $ROUND_N -s $BATCHSLEEP -c $BATCHSIZE $BATCHWAIT --progress $PROGRESS -w $ADMD_ENV_ACTIVATE  $LONGTS -t $OPENMM_CPU_THREADS -l $NSTEPS  -k $TSTEPS -N 0 -b 1 -x 1 --minutes $MINUTES $EXECFLAG $RESCUE_TASKS)

# ONLY TRAJS
elif [[ $WORKLOAD = "trajs" ]]; then
  echo -e "\n ONLY TRAJS"
  echo "python $ADMD_SCRIPT $PROJNAME -R $ROUND_N -s $BATCHSLEEP -c $BATCHSIZE $BATCHWAIT --progress $PROGRESS -w $ADMD_ENV_ACTIVATE $LONGTS -t $OPENMM_CPU_THREADS -l $NSTEPS  -k $TSTEPS -N $NTRAJ -S $SAMPLEFUNC -a $AFTERNTRAJS -b 1 -x 1 --minutes $MINUTES $EXECFLAG $RESCUE_TASKS"
  echo -e "\n\n"
  exitval=$(python $ADMD_SCRIPT $PROJNAME -R $ROUND_N -s $BATCHSLEEP -c $BATCHSIZE $BATCHWAIT --progress $PROGRESS -w $ADMD_ENV_ACTIVATE $LONGTS -t $OPENMM_CPU_THREADS -l $NSTEPS  -k $TSTEPS -N $NTRAJ -S $SAMPLEFUNC -a $AFTERNTRAJS -b 1 -x 1 --minutes $MINUTES $EXECFLAG $RESCUE_TASKS)

# ONLY MODEL
elif [[ $WORKLOAD = "model" ]]; then
  echo -e "\n ONLY MODEL"
  echo "python $ADMD_SCRIPT $PROJNAME -R $ROUND_N -s $BATCHSLEEP -c $BATCHSIZE $BATCHWAIT --progress $PROGRESS -w $ADMD_ENV_ACTIVATE $LONGTS -t $OPENMM_CPU_THREADS -l $NSTEPS  -k $TSTEPS -a $AFTERNTRAJS -N 0 -b 1 -x 1 -M pyemma-invca --min_model_trajlength $MODELTRAJLENGTH --minutes $MINUTES $EXECFLAG $RESCUE_TASKS"
  echo -e "\n\n"
  exitval=$(python $ADMD_SCRIPT $PROJNAME -R $ROUND_N $SYSTEMNAME -s $BATCHSLEEP -c $BATCHSIZE $BATCHWAIT --progress $PROGRESS -w $ADMD_ENV_ACTIVATE $LONGTS -t $OPENMM_CPU_THREADS -l $NSTEPS  -k $TSTEPS -a $AFTERNTRAJS -N 0 -b 1 -x 1 -M pyemma-invca --min_model_trajlength $MODELTRAJLENGTH --minutes $MINUTES $EXECFLAG $RESCUE_TASKS)

# MODEL & TRAJS
elif [[ $WORKLOAD = "all" ]]; then
  echo -e "\n TRAJS & MODEL"
  echo "python $ADMD_SCRIPT $PROJNAME -R $ROUND_N $SYSTEMNAME -s $BATCHSLEEP -c $BATCHSIZE $BATCHWAIT --progress $PROGRESS -w $ADMD_ENV_ACTIVATE $LONGTS -t $OPENMM_CPU_THREADS -l $NSTEPS  -k $TSTEPS -N $NTRAJ -S $SAMPLEFUNC -a $AFTERNTRAJS -b 1 -x 1 -M pyemma-invca --min_model_trajlength $MODELTRAJLENGTH --minutes $MINUTES $EXECFLAG $RESCUE_TASKS"
  echo -e "\n\n"
  exitval=$(python $ADMD_SCRIPT $PROJNAME -R $ROUND_N $SYSTEMNAME -s $BATCHSLEEP -c $BATCHSIZE $BATCHWAIT --progress $PROGRESS -w $ADMD_ENV_ACTIVATE $LONGTS -t $OPENMM_CPU_THREADS -l $NSTEPS  -k $TSTEPS -N $NTRAJ -S $SAMPLEFUNC -a $AFTERNTRAJS -b 1 -x 1 -M pyemma-invca --min_model_trajlength $MODELTRAJLENGTH --minutes $MINUTES $EXECFLAG $RESCUE_TASKS)

else
  echo "Need 1 argument, choice of 'model','trajs','all'"
  echo "Recieved argument: $WORKLOAD"
  exitval="-2"

fi

echo "$exitval"
#echo "AdaptiveMD gave EXITVAL $exitval"
#echo "AdaptiveMD gave EXITVAL "$((exitval))
#exit $((exitval))
