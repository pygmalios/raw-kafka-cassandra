package com.pygmalios.rawKafkaCassandra

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.driver.core.PreparedStatement
import com.pygmalios.rawKafkaCassandra.actors.KafkaToCassandra
import com.pygmalios.rawKafkaCassandra.cassandra.{CassandraSession, CassandraSessionFactory}
import com.pygmalios.rawKafkaCassandra.itest.ITestConfig
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class TestKafkaISpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FlatSpecLike
  with Matchers with BeforeAndAfterAll with ActorSystemRawKafkaCassandraConfig with MockitoSugar {
  import TestKafkaISpec._

  def this() = this(ActorSystem("KafkaToCassandraISpec", ITestConfig.config))
  override def actorSystem = system

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  behavior of "KafkaToCassandra with real Kafka and mocked Cassandra"

  it should "restart in case of Cassandra failure" in new TestScope() {
    // Prepare
    when(mockCassandraSession.write(any(), any())).thenThrow(new RuntimeException("x"))

    // Execute
    run()

    // Wait a little bit to get
    Thread.sleep(15000)

    verify(mockCassandraSession, Mockito.atLeast(3)).write(any(), any())
    verify(mockCassandraSession, Mockito.atLeast(3)).prepareWriteStatement(table1)
    verify(mockCassandraSession, Mockito.atLeast(3)).ensureTableExists(table1)
    verify(mockCassandraSession, Mockito.atLeast(3)).close()
    verify(mockCassandraSessionFactory, Mockito.atLeast(3)).create()
    verifyNoMore()
  }

  class TestScope {
    val mockCassandraSessionFactory = mock[CassandraSessionFactory]
    val mockCassandraSession = mock[CassandraSession]
    when(mockCassandraSessionFactory.create()).thenReturn(mockCassandraSession)
    val preparedStatement1 = mock[PreparedStatement]
    when(mockCassandraSession.prepareWriteStatement(table1)).thenReturn(preparedStatement1)

    def run(): Unit =
      KafkaToCassandra.factory(actorSystem, mockCassandraSessionFactory)

    def verifyNoMore(): Unit = {
      verifyNoMoreInteractions(mockCassandraSession)
      verifyNoMoreInteractions(mockCassandraSessionFactory)
    }
  }
}

object TestKafkaISpec {
  val table1 = "data_source_positioning_device"
}
