package com.pygmalios.rawKafkaCassandra.itest

import com.typesafe.config.ConfigFactory

object ITestConfig {
  lazy val embeddedConfig = ConfigFactory.load("embedded-itest.conf")
  lazy val config = ConfigFactory.load("itest.conf")
}
