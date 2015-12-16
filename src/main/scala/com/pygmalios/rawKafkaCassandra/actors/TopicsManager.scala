package com.pygmalios.rawKafkaCassandra.actors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import com.pygmalios.rawKafkaCassandra.RawKafkaCassandraConfig
import com.pygmalios.rawKafkaCassandra.cassandra.{CassandraSession, CassandraSessionFactory}

class TopicsManager(tableNames: List[String],
                    cassandraSessionFactory: CassandraSessionFactory,
                    tableWriterFactory: (ActorRefFactory, String, CassandraSession) => ActorRef)
  extends Actor with ActorLogging with RawKafkaCassandraConfig {
  private val cassandraSession = cassandraSessionFactory.create()

  override def preStart(): Unit = {
    super.preStart()

    tableNames.foreach { tableName =>
      tableWriterFactory(context, tableName, cassandraSession)
    }
  }

  override def postStop(): Unit = {
    cassandraSession.close()
    super.postStop()
  }

  override val supervisorStrategy = AllForOneStrategy() {
    case ex: Exception =>
      log.error(ex, "StreamingReaderWriter failed!")
      Escalate
  }

  override def receive = Actor.emptyBehavior
}

object TopicsManager {
  def factory(actorRefFactory: ActorRefFactory,
              tableNames: List[String],
              cassandraSessionFactory: CassandraSessionFactory,
              tableWriterFactory: (ActorRefFactory, String, CassandraSession) => ActorRef): ActorRef =
    actorRefFactory.actorOf(Props(new TopicsManager(tableNames, cassandraSessionFactory, tableWriterFactory)), "topicsManager")
}