package io.stored.server.ext.storage

import org.apache.log4j.Logger
import org.h2.jdbcx.JdbcConnectionPool
import org.json.JSONException
import org.json.JSONObject
import java.io.File
import java.sql._
import io.stored.common.SqlUtil
import collection.mutable.{ListBuffer, SynchronizedSet, HashSet}
import io.stored.server.common.{Projection, Record, IndexStorage}
import java.util.UUID


object H2IndexStorage {
  private val log: Logger = Logger.getLogger(classOf[H2IndexStorage])

  def createInMemoryDb : H2IndexStorage = {
    val storage = new H2IndexStorage("mem:%s".format(UUID.randomUUID().toString.replace("-","")))
    storage.init()
    storage
  }

  def create(configRoot: String) : IndexStorage = {
    val storage = new H2IndexStorage(configRoot)
    storage.init()
    storage
  }
}

class H2IndexStorage(configRoot: String) extends IndexStorage {

  private val _tableName = "DATA_INDEX"

  private var _cp: JdbcConnectionPool = null
  private val _tableColumns = new HashSet[String] with SynchronizedSet[String]

  private def getDbFile: String = configRoot + File.separator + "index"
  private def createConnectionString: String = "jdbc:h2:%s".format(if (isMemoryDb) configRoot else getDbFile)
  private def getDbConnection: Connection = _cp.getConnection

  private def getReadOnlyDbConnection: Connection = {
    val conn: Connection = getDbConnection
    conn.setReadOnly(true)
    conn
  }

  def isMemoryDb = configRoot.startsWith("mem:")

  def init() {
    Class.forName("org.h2.Driver")
    _cp = JdbcConnectionPool.create(createConnectionString, "sa", "sa")

    if (isMemoryDb || new File(getDbFile + ".h2.db").exists() != true) {
      bootstrapDb
    }

    loadColumnNames
  }

  def loadColumnNames {
    var db: Connection = null
    var st: Statement = null

    try {
      db = getDbConnection

      val rs: ResultSet = db.getMetaData.getColumns(null, null, _tableName, null)
      while (rs.next) {
        _tableColumns.add(rs.getString(4))
      }
    }
    catch {
      case e: SQLException => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  def shutdown {
    if (_cp != null) _cp.dispose
  }

  private def bootstrapDb {
    var db: Connection = null
    var st: Statement = null
    try {
      db = getDbConnection
      st = db.createStatement
      st.execute("DROP TABLE if exists %s;".format(_tableName))
      st.execute("CREATE TABLE %s ( HASH VARCHAR PRIMARY KEY, RAWDATA VARCHAR );".format(_tableName))
      db.commit
/*
      FullText.init(db)
      FullText.setWhitespaceChars(db, " ,:-._" + File.separator)
      FullText.createIndex(db, "PUBLIC", "DATA_INDEX", null)
*/
    }
    catch {
      case e: SQLException => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  def purge {
    var db: Connection = null
    var st: Statement = null
    try {
      db = getDbConnection
      st = db.createStatement
      st.execute("delete from %s;".format(_tableName))
    } catch {
      case e: SQLException => H2IndexStorage.log.error(e)
    } finally {
      SqlUtil.SafeClose(st)
      SqlUtil.SafeClose(db)
    }
  }

  private def createIndex(db: Connection, tableName: String, colName: String) {
    var statement: PreparedStatement = null
    try {
      val sql = "CREATE INDEX %s_%s ON %s (%s)".format(tableName, colName, tableName, colName)
      println(sql)
      statement = db.prepareStatement(sql)
      statement.execute
    } catch {
      case e: Exception => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(statement)
    }
  }

  private def createColumn(projection: Projection, db: Connection, tableName: String, colName: String, colVal: AnyRef) : Boolean = {
    if (colName.length > 32) return false

    _tableColumns.synchronized {
      if (!_tableColumns.contains(colName)) {
        var statement: PreparedStatement = null
        try {
          var colType: String = null

          if (colVal.isInstanceOf[String]) {
            colType = "VARCHAR"
          } else if (colVal.isInstanceOf[Long]) {
            colType = "LONG"
          } else if (colVal.isInstanceOf[Int]) {
            colType = "LONG"
          } else if (colVal.isInstanceOf[Boolean]) {
            colType = "BOOLEAN"
          } else {
            println(colName + " " + colVal.getClass.toString)
            throw new IllegalArgumentException("unknown obj type: " + colVal.getClass.toString)
          }

          println("adding colname: " + colName + " of type " + colType)

          val sql = "ALTER TABLE %s ADD %s %s NULL".format(tableName, colName, colType)

          statement = db.prepareStatement(sql)
          statement.execute

          if (projection.getFields.contains(colName)) createIndex(db, tableName, colName)

          _tableColumns.add(colName)
        } catch {
          case e: Exception => {
            H2IndexStorage.log.error(e)
          }
        }
        finally {
          SqlUtil.SafeClose(statement)
        }
      }
    }

    true
  }

  private def bind(statement: PreparedStatement, idx: Int, obj: AnyRef) {
    if (obj.isInstanceOf[String]) {
      statement.setString(idx, obj.asInstanceOf[String])
    } else if (obj.isInstanceOf[Long]) {
      statement.setLong(idx, obj.asInstanceOf[Long])
    } else if (obj.isInstanceOf[Int]) {
      statement.setInt(idx, obj.asInstanceOf[Int])
    } else if (obj.isInstanceOf[Boolean]) {
      statement.setBoolean(idx, obj.asInstanceOf[Boolean])
    } else {
      throw new IllegalArgumentException("unknown obj type: " + obj.toString)
    }
  }

  def addAll(projection: Projection, nodeIdMap: Map[Int, Set[String]], recordMap: Map[String, Record]) : List[String] = {
    addAll(projection, nodeIdMap, recordMap.values.toList)
  }

  def addAll(projection: Projection, nodeIdMap: Map[Int, Set[String]], records: List[Record]) : List[String] = {
    var db: Connection = null
    var statement: PreparedStatement = null

    try {
      db = getDbConnection
      db.setAutoCommit(false)

      var i = 0
      var lastSql : String = null

      val ids = records.map{ orec =>
        val record = if (orec.colMap == null) { Record.create(orec.id, orec.rawData) } else { orec }
        val colMap = record.colMap.filter{ (col: (String, AnyRef)) =>
          (_tableColumns.contains(col._1) || createColumn(projection, db, _tableName, col._1, record.colMap.get(col._1).get))
        }
        if (colMap.size == 0) throw new IllegalArgumentException("filtered colMap has no data to index")
        val cols = colMap.keySet.toList

        val sql = "MERGE INTO %s (HASH,RAWDATA,%s) VALUES (?,?,%s);".format(
          _tableName,
          cols.mkString(","),
          List.fill(cols.size)("?").mkString(","))

        if (lastSql == null) {
          statement = db.prepareStatement(sql)
        } else if (sql != lastSql) {
          statement.executeBatch()
          SqlUtil.SafeClose(statement)
          statement = db.prepareStatement(sql)
        }
        lastSql = sql

        bind(statement, 1, record.id)
        bind(statement, 2, record.rawData.toString)
        (0 until cols.size).foreach(idx => bind(statement, idx+3, colMap.get(cols(idx)).get))

        statement.addBatch

        i += 1
        if (i > 1024) {
          statement.executeBatch()
          i = 0
        }

        record.id
      }

      statement.executeBatch()

      db.commit
      ids
    } catch {
      case e: JSONException => { H2IndexStorage.log.error(e); null }
      case e: SQLException => { H2IndexStorage.log.error(e); null }
    } finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  def remove(projection: Projection, nodeIds: Set[Int], ids: List[String]) {
    var db: Connection = null
    var statement: PreparedStatement = null

    try {
      db = getDbConnection
      db.setAutoCommit(false)

      statement = db.prepareStatement("DELETE FROM %s WHERE HASH = ?;".format(_tableName))

      var i = 0

      ids.foreach{ id =>
        bind(statement, 1, id)

        i += 1
        if (i > 1024) {
          statement.executeBatch()
          i = 0
        }
      }

      statement.executeBatch()

      db.commit
    } catch {
      case e: JSONException => H2IndexStorage.log.error(e)
      case e: SQLException => H2IndexStorage.log.error(e)
    } finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
  }

  def query(projection: Projection, nodeIds: Set[Int], sql: String): List[Record] = {
    val results = new ListBuffer[Record]

    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getReadOnlyDbConnection
      statement = db.prepareStatement(sql)
      val rs: ResultSet = statement.executeQuery
      while (rs.next) results.append(new Record(rs.getString("HASH"), null, new JSONObject(rs.getString("RAWDATA"))))
    } catch {
      case e: JSONException => H2IndexStorage.log.error(e)
      case e: SQLException => H2IndexStorage.log.error(e)
    } finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }

    results.toList
  }
}