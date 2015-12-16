package com.pygmalios.rawKafkaCassandra.cassandra

import com.datastax.driver.core.{ProtocolOptions, Cluster}
import com.pygmalios.rawKafkaCassandra.GenericRawKafkaCassandraConfig

trait CassandraSessionFactory {
  def create(): CassandraSession
}

class CassandraSessionFactoryImpl(config: GenericRawKafkaCassandraConfig) extends CassandraSessionFactory {
  override def create(): CassandraSession = {
    val cluster = Cluster
      .builder()
      .addContactPoints(config.cassandraHostsConfig: _*)
      .withCompression(ProtocolOptions.Compression.SNAPPY)
      .withPort(config.cassandraPortConfig)
      .build()

    if (cluster.getMetadata.getKeyspace(config.cassandraKeyspace) == null) {
      val keyspaceSession = cluster.connect()
      try {
        keyspaceSession.execute(s"CREATE KEYSPACE ${config.cassandraKeyspace} WITH replication =" +
          "{'class': 'SimpleStrategy', 'replication_factor': '2'}  AND durable_writes = true;")
      }
      finally {
        keyspaceSession.close()
      }
    }

    // Connect to session
    new CassandraSessionImpl(cluster.connect(config.cassandraKeyspace), config)
  }
}
