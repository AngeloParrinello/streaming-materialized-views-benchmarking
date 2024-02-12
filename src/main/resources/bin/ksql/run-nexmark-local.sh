docker compose up -d

# shellcheck disable=SC2181
if [ $? -eq 0 ]; then
  echo "Docker Compose containers are up and running."

  # check if the topics already exist
  topics=("people" "auctions" "bids")

  # Iterate through the topics
  for topic in "${topics[@]}"; do
      if docker exec -it broker kafka-topics --bootstrap-server broker:29092 --list | grep -q "$topic"; then
          echo "Topic $topic already exists."
      else
          echo "Topic $topic does not exist."
          echo "I'm going to create the topics and the data on Kafka."
          # create the topics and the data on Kafka if they do not exist
            docker exec -it streaming-materialized-views-benchmarking-jobmanager-1 \
              bash -c 'cd bin/ && flink run build/libs/EventsOnKafkaJob.jar && exit'
          # now that we are sure that the topics exist, we can exit the loop
          break
      fi
  done

  echo "I'm going to create the types and the streams on ksqlDB."

  docker exec -it ksqldb-cli ksql http://ksqldb-server:8088 --file queries/ksql/create_types.sql

  echo "I'm going to create the queries on ksqlDB."

  # Define an array of query file names
  queries=("query1.sql" "query2.sql" "query3.sql" "query4.sql" "query5.sql" "query6.sql" "query7.sql" "query8.sql")

  # Iterate through the queries and run them
  for query in "${queries[@]}"; do


      docker exec -it ksqldb-cli ksql http://ksqldb-server:8088 --file queries/ksql/"$query"

      echo "Query $query has finished."
  done

else
  echo "Docker Compose failed to start containers."

fi





