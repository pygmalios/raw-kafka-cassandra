package com.pygmalios.rawKafkaCassandra

import akka.actor.{Actor, ActorSystem}
import com.typesafe.config.Config

import scala.collection.JavaConversions._

/**
  * Configuration.
  */
trait GenericRawKafkaCassandraConfig {
  def config: Config

  // Kafka configuration
  def kafkaConfig = config.getConfig("kafka")
  def kafkaBrokersConfig: List[String] = kafkaConfig.getStringList("brokers").toList
  def kafkaZooKeeperHostConfig = kafkaConfig.getString("zooKeeperHost")
  def kafkaTopicsConfig: List[String] = kafkaConfig.getString("topics").split(",").toList
  def kafkaConsumerGroupIdConfig = kafkaConfig.getString("consumerGroupId")

  // Cassandra configuration
  def cassandraConfig = config.getConfig("cassandra")
  def cassandraPortConfig: Int = cassandraConfig.getInt("port")
  def cassandraHostsConfig: List[String] = cassandraConfig.getStringList("hosts").toList
  def cassandraKeyspace = cassandraConfig.getString("keyspace")
}

trait ActorSystemRawKafkaCassandraConfig extends GenericRawKafkaCassandraConfig {
  def actorSystem: ActorSystem
  override def config = actorSystem.settings.config.getConfig("raw-kafka-cassandra")
}

trait RawKafkaCassandraConfig extends ActorSystemRawKafkaCassandraConfig {
  self: Actor =>
  override def actorSystem: ActorSystem = context.system
}

class SimpleRawKafkaCassandraConfig(val actorSystem: ActorSystem) extends ActorSystemRawKafkaCassandraConfig