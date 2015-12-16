package com.pygmalios.rawKafkaCassandra.test

import akka.actor.{Actor, ActorLogging}
import akka.event.LoggingReceive

import scala.concurrent.duration._

object TestUtils {
  val expectedDuration = 0.1.seconds
}

class DummyActor(tableName: String) extends Actor {
  override def receive: Receive = Actor.emptyBehavior
}

class ThrowActor extends Actor with ActorLogging {
  override def receive = LoggingReceive {
    case ex: Exception => throw ex
  }
}
