#!/bin/bash

# Launch the metrics reporter
run_reporter() {
  cd ../../../../..

  # run the reporter with java 11
  # shellcheck disable=SC1001
  java -jar build/libs/FlinkReporter.jar
}

docker-compose up -d

# shellcheck disable=SC2181
if [ $? -eq 0 ]; then
  echo "Docker Compose containers are up and running."

  docker exec -it streaming-materialized-views-benchmarking-jobmanager-1 \
    bash -c 'cd bin/src/main/resources/bin/flink && ./run-queries.sh'

  if [ $? -eq 0 ]; then
    echo "Flink Nexmark Jobs have started."

    while ! docker exec streaming-materialized-views-benchmarking-jobmanager-1 bash -c 'test -e bin/pipe'; do
      sleep 100
    done

    echo "Script has finished."

    run_reporter

    # docker-compose down

  else
    echo "Failed to start a Flink Nexmark Job."
  fi
else
  echo "Docker Compose failed to start containers."
fi
