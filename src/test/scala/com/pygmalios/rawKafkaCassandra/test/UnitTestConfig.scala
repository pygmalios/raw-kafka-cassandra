package com.pygmalios.rawKafkaCassandra.test

import com.typesafe.config.ConfigFactory

/**
  * Created by rado on 12/10/15.
  */
object UnitTestConfig {
  lazy val config = ConfigFactory.load("unit-test.conf")
}
