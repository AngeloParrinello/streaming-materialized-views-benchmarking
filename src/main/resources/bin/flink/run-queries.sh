#!/bin/bash

# This script will be run inside the job manager (the master) of the cluster

cd ../../../../..

# create the events on Kafka
flink run build/libs/EventsOnKafkaJob.jar

# run Flink jobs sequentially
for number in {1..8}; do
  flink run build/libs/QueryJob.jar --queryNumber "$number"

  BACK_PID=$!

  wait $BACK_PID

  echo "Query $number has finished."

done

# create a file to signal the end of the Flink jobs
echo "Flink jobs have finished."
touch pipe

exit


