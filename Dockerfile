FROM niaquinto/gradle
MAINTAINER Rado Buransky <radoburansky@gmail.com>

# Copy and build sources
COPY ./ /usr/raw-kafka-cassandra
WORKDIR /usr/raw-kafka-cassandra
RUN gradle clean installDist

# Reset parent container behavior
ENTRYPOINT []
CMD []