package com.pygmalios.rawKafkaCassandra.actors

import akka.actor._
import akka.event.LoggingReceive
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.pygmalios.rawKafkaCassandra.RawKafkaCassandraConfig
import com.pygmalios.rawKafkaCassandra.cassandra.CassandraSession
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import kafka.serializer.StringDecoder

import scala.concurrent.Future
import scala.concurrent.duration._

class StreamingReaderWriter(kafkaTopicName: String,
                            cassandraSession: CassandraSession)
  extends Actor with ActorLogging with RawKafkaCassandraConfig {
  import StreamingReaderWriter._

  private val cassandraDispatcher = context.system.dispatchers.lookup("raw-kafka-cassandra.cassandra.dispatcher")
  private val decider: Supervision.Decider = {
    case ex: Exception =>
      self ! StreamingFailed(ex)
      Supervision.stop
  }
  private val materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider))

  // Order matters here! You cannot prepare a write statement if the table doesn't already exist.
  cassandraSession.ensureTableExists(tableName)
  private val writeStatement = cassandraSession.prepareWriteStatement(tableName)

  override def preStart(): Unit = {
    super.preStart()
    self ! StartStreaming
  }

  override def postStop() = {
    materializer.shutdown()
    super.postStop()
  }

  override def receive: Receive = LoggingReceive {
    case StartStreaming =>
      Future {
        // Create Kafka consumer properties
        val consumerProperties = ConsumerProperties(
          brokerList = kafkaBrokersConfig.mkString(","),
          zooKeeperHost = kafkaZooKeeperHostConfig,
          topic = kafkaTopicName,
          groupId = kafkaConsumerGroupIdConfig,
          new StringDecoder())
          .setProperty("rebalance.backoff.ms", "8000")
          .setProperty("zookeeper.session.timeout.ms", "8000")
          .readFromEndOfStream()
          .commitInterval(5.seconds)

        // Create consumer
        val consumer = new ReactiveKafka().consumeWithOffsetSink(consumerProperties)(context.system)

        // Create graph and run it
        Source(consumer.publisher)
          .map(write)
          .to(consumer.offsetCommitSink)
          .run()(materializer)

        log.debug(s"Streaming started [$kafkaTopicName].")

        // Change state to avoid another start
        context.become(streaming)
      }(cassandraDispatcher).onFailure {
        case ex: Exception => throw ex
      }(cassandraDispatcher)
  }

  def streaming: Receive = LoggingReceive {
    case StreamingFailed(ex) => throw new StreamingException(ex)
  }

  private def write(kafkaMessage: KafkaMessage[String]): KafkaMessage[String] =  {
    log.debug("Writing Kafka message to Cassandra: " + kafkaMessage)
    cassandraSession.write(writeStatement, kafkaMessage.message())
    kafkaMessage
  }

  private def tableName = kafkaTopicName.replace(".", "_").replace("-", "_").toLowerCase
}

object StreamingReaderWriter {
  private case object StartStreaming
  private case class StreamingFailed(cause: Exception)
  private class StreamingException(cause: Throwable) extends RuntimeException(cause)

  def factory(actorRefFactory: ActorRefFactory, tableName: String, cassandraSession: CassandraSession): ActorRef =
    actorRefFactory.actorOf(Props(new StreamingReaderWriter(tableName, cassandraSession)), tableName)
}