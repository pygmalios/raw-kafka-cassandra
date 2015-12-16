package com.pygmalios.rawKafkaCassandra

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.pygmalios.rawKafkaCassandra.actors.KafkaToCassandra
import com.pygmalios.rawKafkaCassandra.cassandra.CassandraSessionFactoryImpl
import com.pygmalios.rawKafkaCassandra.itest.ITestConfig
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import collection.JavaConversions._
import scala.concurrent.duration._

class EmbeddedKafkaToLocalCassandraISpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with FlatSpecLike
  with Matchers with BeforeAndAfterAll with ActorSystemRawKafkaCassandraConfig with EmbeddedKafka with MockitoSugar {
  import EmbeddedKafkaToLocalCassandraISpec._

  def this() = this(ActorSystem("KafkaToCassandraISpec", ITestConfig.embeddedConfig))

  override def actorSystem = system
  private lazy val cassandraSessionFactory = new CassandraSessionFactoryImpl(this)
  private lazy val cassandraSession = cassandraSessionFactory.create()

  override def afterAll: Unit = {
    cassandraSession.execute(s"DROP KEYSPACE $cassandraKeyspace;")
    cassandraSession.close()
    TestKit.shutdownActorSystem(system)
  }

  behavior of "KafkaToCassandra with embedded Kafka and local Cassandra"

  it should "write two Kafka messages to two tables" in {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.format(new Date())

    cassandraSession.execute(s"DROP TABLE $cassandraKeyspace.$topic1;")
    cassandraSession.execute(s"DROP TABLE $cassandraKeyspace.$topic2;")

    withRunningKafka {
      // Execute
      KafkaToCassandra.factory(actorSystem, cassandraSessionFactory)

      // Embedded Kafka needs some time to spin up...
      Thread.sleep(5000)

      // Publish some messages
      publishStringMessageToKafka(topic1, "1a")
      publishStringMessageToKafka(topic2, "2a")

      // Wait a little bit ...
      awaitAssert(assertSingleRow(topic1, "1a"), 10.seconds, 1.second)
      awaitAssert(assertSingleRow(topic2, "2a"), 10.seconds, 1.second)
    }

    def assertSingleRow(topicName: String, msg: String): Unit = {
      val rows = cassandraSession.execute(s"SELECT * FROM $cassandraKeyspace.$topicName;").all().toList
      assert(rows.size == 1, s"$topicName, $msg")

      val row = rows.head
      assert(row.getString("msg") == msg, s"$topicName, $msg")
      assert(row.getString("date") == date, s"$topicName, $date")
    }
  }
}

object EmbeddedKafkaToLocalCassandraISpec {
  val topic1 = "test1"
  val topic2 = "test2"
}