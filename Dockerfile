FROM niaquinto/gradle
MAINTAINER Rado Buransky <radoburansky@gmail.com>

# Copy and build sources
COPY ./ /usr/raw-kafka-cassandra
WORKDIR /usr/raw-kafka-cassandra
RUN gradle clean installDist

# This step assumes that you have already build the application using `gradle installDist`
#COPY ./build/install/raw-kafka-cassandra /usr/raw-kafka-cassandra

# This step assumes that you have your own config named `raw-kafka-cassandra.conf` in the root directory
COPY ./raw-kafka-cassandra.conf /usr/raw-kafka-cassandra

# Set environment variable to use the provided config file
ENV RAW_KAFKA_CASSANDRA_OPTS="-Dconfig.file=/usr/raw-kafka-cassandra/raw-kafka-cassandra.conf

# Reset parent container behavior
ENTRYPOINT []
CMD []