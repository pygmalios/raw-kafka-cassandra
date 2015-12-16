package com.pygmalios.rawKafkaCassandra.actors

import akka.actor._
import com.pygmalios.rawKafkaCassandra.RawKafkaCassandraConfig
import com.pygmalios.rawKafkaCassandra.akka24.BackoffSupervisor
import com.pygmalios.rawKafkaCassandra.cassandra.CassandraSessionFactory

import scala.concurrent.duration._

/**
  * Top-level actor.
  */
class KafkaToCassandra(cassandraSessionFactory: CassandraSessionFactory) extends Actor with ActorLogging with RawKafkaCassandraConfig {
  private val cassandraClusterBackoffSupervisor = backoffSupervisorProps(
    Props(new TopicsManager(kafkaTopicsConfig, cassandraSessionFactory, StreamingReaderWriter.factory)),
    "topicsManager")

  override def receive = Actor.emptyBehavior

  private def backoffSupervisorProps(childProps: Props, childName: String): ActorRef =
    // TODO: Configurable
    context.actorOf(BackoffSupervisor.props(
      childProps = childProps,
      childName = childName,
      minBackoff = 0.1.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2))
}

object KafkaToCassandra {
  def factory(actorRefFactory: ActorRefFactory, cassandraSessionFactory: CassandraSessionFactory): ActorRef =
    actorRefFactory.actorOf(Props(new KafkaToCassandra(cassandraSessionFactory)), "kafkaToCassandra")
}