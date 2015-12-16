package com.pygmalios.rawKafkaCassandra

import akka.actor.ActorSystem
import com.pygmalios.rawKafkaCassandra.actors.KafkaToCassandra
import com.pygmalios.rawKafkaCassandra.cassandra.CassandraSessionFactoryImpl

/**
  * Application entry point.
  *
  * To load an external config file named `raw-kafka-cassandra.config`,
  * provide `-Dconfig.file=raw-kafka-cassandra.config` argument to JVM.
  */
object RawKafkaCassandraApp extends App {
  // Bootstrap
  val actorSystem = ActorSystem("raw-kafka-cassandra")
  val config = new SimpleRawKafkaCassandraConfig(actorSystem)
  val cassandraSessionFactory = new CassandraSessionFactoryImpl(config)

  // Create root KafkaToCassandra actor
  KafkaToCassandra.factory(actorSystem, cassandraSessionFactory)
}
