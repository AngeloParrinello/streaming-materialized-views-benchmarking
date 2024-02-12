# Streaming materialized views benchmarking

This repository contains the source code for the thesis "Streaming materialized views benchmarking"
by Parrinello Angelo.
The thesis has been written in the context of [Agile Lab](https://www.agilelab.it/) and
the [University of Bologna](https://www.unibo.it/it).

This project aims to benchmark the performance of different systems which are able to provides streaming materialized
views.

## Description

With the growing significance of real-time analytics,
streaming databases have emerged as essential tools for data processing.
Among the prevailing paradigms, the "streaming materialized view" stands
out for its simplicity and effectiveness. We compare the performance of
[Flink](https://flink.apache.org/), [ksqlDB](https://ksqldb.io/) and
[Materialize](https://materialize.com/).

The benchmark uses the [Nexmark Benchmark](https://github.com/nexmark/nexmark)'s data generator
as well as the data model. The benchmark is composed by a set of queries
which are executed on the three systems.

### Queries

The queries are the original
[NEXmark queries](https://web.archive.org/web/20100620010601/http://datalab.cs.pdx.edu/niagaraST/NEXMark/)
with some minor modifications.

The queries are defined for a benchmark that has been designed with traditional stream processors in mind rather than an operational data warehouse like Materialize. So keep in mind that. 

| Query | Name                            | Summary                                                                            | Flink                                                                                     | ksqlDB                                                                                                                                                                                                                                                                                     | Materialize |
|-------|---------------------------------|------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| q1    | Currency Conversion             | Convert each bid value from dollars to euros.                                      | ✅                                                                                         | ✅                                                                                                                                                                                                                                                                                          | ✅           |
| q2    | Selection                       | Find bids with specific auction ids and show their bid price.                      | ✅                                                                                         | ✅                                                                                                                                                                                                                                                                                          | ✅           |
| q3    | Local Item Suggestion           | Who is selling in OR, ID or CA in category 10, and for what auction ids?           | ✅                                                                                         | ✅                                                                                                                                                                                                                                                                                          | ✅           |
| q4    | Average Price for a Category    | Select the average of the wining bid prices for all auctions in each category.     | ✅                                                                                         | ✅                                                                                                                                                                                                                                                                                          | ✅           |
| q5    | Hot Items                       | Which auctions have seen the most bids in the last period?                         | ✅                                                                                         | ❌ (Not supported due to [ksqlDB-6513](https://github.com/confluentinc/ksql/issues/6513), [ksqlDB-3984](https://github.com/confluentinc/ksql/issues/3984), [ksqlDB-5519](https://github.com/confluentinc/ksql/issues/5519), [ksqlDB-8574](https://github.com/confluentinc/ksql/issues/8574) | ✅           |
| q6    | Average Selling Price by Seller | What is the average selling price per seller for their last 10 closed auctions.    | ❌ (Not supported due to [FLINK-19059](https://issues.apache.org/jira/browse/FLINK-19059)) | ✅                                                                                                                                                                                                                                                                                          | ✅           |
| q7    | Highest Bid                     | Select the bids with the highest bid price in the last period.                     | ✅                                                                                         | ✅                                                                                                                                                                                                                                                                                          | ✅           |
| q8    | Monitor New Users               | Select people who have entered the system and created auctions in the last period. | ✅                                                                                         | ✅                                                                                                                                                                                                                                                                                          | ✅           |

### Metrics

In the context of Nexmark's performance evaluation, two key metrics are employed: throughput and time.

- **Throughput**: Throughput quantifies the rate at which events are processed per second. This metric is derived from the
total number of events generated by each data source. For instance, if a query originates from the bids topic,
containing 100 events, and it takes 10 seconds to execute, the throughput is calculated as 10 events per second.
Throughput is assessed for each query and across various systems, with measurements expressed in both bytes per second
and events/rows per second.

- **Time**: Time refers to the duration taken to execute a query, and it is measured in seconds. This metric provides insights
into the efficiency and speed of query completion.

## Badges

TODO

## Visuals

TODO (add some screenshots or GIFs)

## Installation

### Prerequisites

#### For local execution

Before you can get started with this project, you need to ensure that your system meets the following prerequisites:

- Docker: This project relies on Docker for containerization. If you haven't already installed Docker, you can find
installation instructions for your specific platform here [Docker Installation Guide](https://docs.docker.com/get-docker/).

- Docker Compose: You'll need to install Docker Compose. You can find installation instructions here: [Docker Compose Installation Guide](https://docs.docker.com/compose/install/).

Once you've met these prerequisites, you'll be ready to set up and run this project on your system.

### How to install

TODO

## Configuration

### Remote machines

The benchmark has been executed on an AWS remote cluster, for what concern Flink and ksqlDB.
The cluster is composed by four machines (one master and three slaves). On the master node has been
installed the JobManager (Flink), the ksqlDB client and Grafana/Prometheus (ksqlDB). Each instance with the following characteristics:
- m5a.2xlarge instance type
- Ubuntu Server 22.04 LTS (HVM), SSD Volume Type
- 8 vCPU
- 32 GB RAM
- 100 GB SSD

The benchmark has been executed on Materialize cloud service for what concern Materialize.
One machine composes the cluster with the following characteristics:
- medium instance type
- 1 processes
- 12 workers
- 14 vCPU
- ~ 120 GB RAM
- ~ 540 GB SSD

We used AWS MSK as Kafka cluster with the following characteristics:
- 3 brokers
- 3 m5.large instance type
- 200 GB SSD

### Flink

The Flink's configuration used in the benchmark is defined in the file
`flink-conf.yaml` in the `conf` folder.

Some notable configurations including:

- 8 TaskManagers, each has only 1 slot
- 2 GB for each TaskManager and JobManager (TODO: check if it is enough)
- Job parallelism: 8
- Checkpoint enabled with exactly once mode and 3-minute interval
- Use RocksDB state backend with incremental checkpoint enabled
- MiniBatch optimization enabled with 2 second intervals and 50000 rows
- Splitting distinct aggregation optimization is enabled
- High Availability is not enabled (should be done with ZooKeeper and and a persistent
  remote storage, such as HDFS, NFS, or S3)
- Disabled operator chaining
- DML queries are run in sync mode not in async mode (which is the default)

### ksqlDB

The ksqlDB's configuration used in the benchmark is defined in the file
`ksql-server.properties` in the `conf` folder.

Some notable configurations including:
- The replication factor of the internal topics is set to 3 (TODO: check if it is enough and if it is true) for
a better fault tolerance
- The monitoring is enabled

### Materialize

We used the default configuration of Materialize, in other words, we did not change any configuration.

## Usage

### Local

#### Flink

To run the benchmark locally with Flink, you may want to change the number of the events produced by the generator on
Kafka and the delivery guarantee of the queries, modifying the Nexmark's configuration file `nexmark-conf.yaml` in the
`conf` folder. Then, you need to start the Docker engine and run the benchmark with the following commands:


```shell
cd src/main/resources/bin/flink/ && ./run-nexmark-local.sh
```

The script will launch the Flink Nexmark benchmark with the desired configuration. Each query corresponds to a
different Flink job: each one will be executed consecutively, with a parallelism of 4. The script will also create a
.csv file with the results of the benchmark. The file will be saved in the root folder of the project.

You can also run a single query with the following command:

```shell
docker exec -it streaming-materialized-views-benchmarking-jobmanager-1

# Then, inside the container
cd bin && flink run build/libs/EventsOnKafkaJob.jar

# After the Kafka cluster is up and running and has got the events, run the query
flink run build/libs/QueryJob.jar --queryNumber <query_number> --parallelism <parallelism>
```

#### ksqlDB

To run the benchmark locally with ksqlDB, you may want to change the number of the events produced by the generator on
Kafka and the delivery guarantee of the queries, modifying the Nexmark's configuration file `nexmark-conf.yaml` in the
`conf` folder. Then, you need to start the Docker engine and run the benchmark with the following commands:

```shell
cd src/main/resources/bin/ksql/ && ./run-nexmark-local.sh
```

As before, the script will launch the ksqlDB Nexmark benchmark with the desired configuration, all the queries will be
executed. However, the queries are launched in parallel and not consecutively. The fact that the queries are launched
in parallel alters the results of the benchmark local because they compete for the same resources. This is a known issue
and will be fixed in the future.

Also in this case, you can run a single query with the following command:

```shell
# If the events are not already on Kafka
docker exec -it streaming-materialized-views-benchmarking-jobmanager-1
# Then, inside the container
cd bin && flink run build/libs/EventsOnKafkaJob.jar
exit

docker exec -it ksqldb-cli ksql http://ksqldb-server:8088 
# Then inside the ksqlDB CLI
RUN SCRIPT 'home/appuser/queries/ksql/create_types.sql';
RUN SCRIPT 'home/appuser/queries/ksql/query<number>.sql';
```

#### Materialize

Currently, we have not implemented a script to run the benchmark locally with Materialize. However, you can run the
single queries in the test folder `src/test/java/it/agilelab/thesis/nexmark/materialize/NexmarkQueriesMaterializeTest.java`.

Or through Gradle with the following command:
```shell
./gradlew test --tests it.agilelab.thesis.nexmark.materialize.NexmarkQueriesMaterializeTest
```

Alternatively, you can run the queries manually with the following commands:

```shell
# Let's suppose that the events are already on Kafka
docker compose run cli 
# Then, inside the container
\i /home/appuser/queries/materialize/create_types.sql
\i /home/appuser/queries/materialize/query<number>.sql  
```

### Remote
TODO

## Monitoring
As monitoring stack we used [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/).
TODO

Moreover REST API jmx ecc. TODO

## Roadmap

TODO

## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. So,
any contributions you make are **greatly appreciated**.

## Project status

The project is still in development and is not ready for production.

## License

This project is available under the [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0);
see [LICENSE](LICENSE) for full details.

## About Agile Lab

<p align="center">
    <a href="https://www.agilelab.it">
        <img src="docs/img/agilelab_logo.jpg" alt="Agile Lab" width=600>
    </a>
</p>

Agile Lab creates value for its Clients in data-intensive environments through customizable solutions to establish
performance driven processes, sustainable architectures, and automated platforms driven by data governance best
practices.

Since 2014 we have implemented 100+ successful Elite Data Engineering initiatives and used that experience to create
Witboost: a technology agnostic, modular platform, that empowers modern enterprises to discover, elevate and productize
their data both in traditional environments and on fully compliant Data mesh architectures.

[Contact us](https://www.agilelab.it/contacts) or follow us on:

- [LinkedIn](https://www.linkedin.com/company/agile-lab/)
- [Instagram](https://www.instagram.com/agilelab_official/)
- [YouTube](https://www.youtube.com/channel/UCTWdhr7_4JmZIpZFhMdLzAA)
- [Twitter](https://twitter.com/agile__lab)