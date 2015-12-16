package com.pygmalios.rawKafkaCassandra

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.driver.core.{PreparedStatement, ResultSet}
import com.pygmalios.rawKafkaCassandra.actors.KafkaToCassandra
import com.pygmalios.rawKafkaCassandra.cassandra.{CassandraSession, CassandraSessionFactory}
import com.pygmalios.rawKafkaCassandra.itest.ITestConfig
import net.manub.embeddedkafka.EmbeddedKafka
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class EmbeddedKafkaISpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FlatSpecLike
  with Matchers with BeforeAndAfterAll with ActorSystemRawKafkaCassandraConfig with EmbeddedKafka with MockitoSugar {
  import EmbeddedKafkaISpec._

  def this() = this(ActorSystem("KafkaToCassandraISpec", ITestConfig.embeddedConfig))

  override def actorSystem = system

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  behavior of "KafkaToCassandra with embedded Kafka and mocked Cassandra"

  it should "stream data from embedded Kafka to mocked Cassandra" in new TestScope() {
    // Prepare
    when(mockCassandraSession.write(any(), any())).thenReturn(mock[ResultSet])

    withRunningKafka {
      // Execute
      run()

      // Publish some messages
      publishStringMessageToKafka("test1", "1a")
      publishStringMessageToKafka("test2", "2a")
      publishStringMessageToKafka("test1", "1b")
      publishStringMessageToKafka("test2", "2b")

      // Wait a little bit ...
      Thread.sleep(100)
    }

    // Verify
    verify(mockCassandraSession).write(preparedStatement1, "1a")
    verify(mockCassandraSession).write(preparedStatement2, "2a")
    verify(mockCassandraSession).write(preparedStatement1, "1b")
    verify(mockCassandraSession).write(preparedStatement2, "2b")
    verify(mockCassandraSession).prepareWriteStatement(topic1)

    verifyCommon()
    verifyNoMore()
  }

  class TestScope {
    val mockCassandraSessionFactory = mock[CassandraSessionFactory]
    val mockCassandraSession = mock[CassandraSession]
    when(mockCassandraSessionFactory.create()).thenReturn(mockCassandraSession)
    val preparedStatement1 = mock[PreparedStatement]
    val preparedStatement2 = mock[PreparedStatement]
    when(mockCassandraSession.prepareWriteStatement(topic1)).thenReturn(preparedStatement1)
    when(mockCassandraSession.prepareWriteStatement(topic2)).thenReturn(preparedStatement2)

    def run(): Unit =
      KafkaToCassandra.factory(actorSystem, mockCassandraSessionFactory)

    def verifyCommon(): Unit = {
      verify(mockCassandraSession).prepareWriteStatement(topic2)
      verify(mockCassandraSession).ensureTableExists(topic1)
      verify(mockCassandraSession).ensureTableExists(topic2)
      verify(mockCassandraSessionFactory).create()
    }

    def verifyNoMore(): Unit = {
      verifyNoMoreInteractions(mockCassandraSession)
      verifyNoMoreInteractions(mockCassandraSessionFactory)
    }
  }
}

object EmbeddedKafkaISpec {
  val topic1 = "test1"
  val topic2 = "test2"
}