package com.pygmalios.rawKafkaCassandra.cassandra

import java.io.Closeable
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.datastax.driver.core._
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import com.pygmalios.rawKafkaCassandra.GenericRawKafkaCassandraConfig
import org.slf4j.LoggerFactory

trait CassandraSession extends Closeable {
  def prepareWriteStatement(fullTableName: String): PreparedStatement
  def ensureTableExists(tableName: String): Unit
  def write(writeStatement: PreparedStatement, message: String): ResultSet
  def execute(statement: String): ResultSet
}

class CassandraSessionImpl(session: Session, config: GenericRawKafkaCassandraConfig) extends CassandraSession {
  import CassandraSessionImpl._

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  override def prepareWriteStatement(tableName: String): PreparedStatement = {
    val fullTableName = session.getLoggedKeyspace + "." + tableName
    session.prepare(s"INSERT INTO $fullTableName($dateColumn, $timestampColumn, $msgColumn) VALUES (?, ?, ?);")
      .setConsistencyLevel(ConsistencyLevel.ONE)
  }

  override def ensureTableExists(tableName: String): Unit = {
    val createTableStatement = SchemaBuilder.createTable(session.getLoggedKeyspace, tableName)
      .addPartitionKey(dateColumn, DataType.text())
      .addClusteringColumn(timestampColumn, DataType.timestamp())
      .addColumn(msgColumn, DataType.text())
      .ifNotExists()

    // Execute synchronously
    session.execute(createTableStatement)
  }

  override def write(writeStatement: PreparedStatement, message: String): ResultSet = {
    val now = new Date()
    val result = session.execute(writeStatement.bind(dateFormat.format(now), now, message))
    log.debug("Message written: " + message)
    result
  }

  override def execute(statement: String): ResultSet = {
    val preparedStatement = session.prepare(statement).setConsistencyLevel(ConsistencyLevel.ONE)
    session.execute(preparedStatement.bind())
  }

  override def close(): Unit = {
    val cluster = session.getCluster
    session.close()
    cluster.close()
  }
}

object CassandraSessionImpl {
  private val dateColumn = "date"
  private val timestampColumn = "timestamp"
  private val msgColumn = "msg"
}