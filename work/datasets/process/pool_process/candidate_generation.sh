#!/bin/bash
start=`date +%s`
num_procs=$1
num_jobs="\j" 


echo "Candidate Retrival Started!"
i=0
while ((!STOP)); do
  while (( ${num_jobs@P} >= num_procs )); do
    wait -n
  done
  ((i++))
  echo "Process job: ${i}"
  python ./candidate_generation.py &
done
wait
