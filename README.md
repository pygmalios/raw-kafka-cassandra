# raw-kafka-cassandra
Persists raw Kafka data to Cassandra.

## Build distribution and run it

1. Use Gradle to build a runnable application: `gradle installDist`
2. Set optional arguments: `$ export RAW_KAFKA_CASSANDRA_OPTS="-Dconfig.file=/home/rado/raw-kafka-cassandra.config"`
3. Run script: `$ ./build/install/raw-kafka-cassandra/bin/raw-kafka-cassandra`

## Create Docker image and run it

1. Execute from project root to create the image: `$ docker build -t pygmalios/raw-kafka-cassandra .`
2. Run the Docker image: `$ docker run --name raw-kafka-cassandra -dit pygmalios/raw-kafka-cassandra "/usr/raw-kafka-cassandra/bin/raw-kafka-cassandra"`