package io.stored.server.storage

import org.apache.log4j.Logger
import org.h2.jdbcx.JdbcConnectionPool
import org.json.JSONException
import org.json.JSONObject
import java.io.File
import java.sql._
import io.stored.server.common.{Record, IndexStorage}
import io.stored.common.SqlUtil
import collection.mutable.{ListBuffer, SynchronizedSet, HashSet}

object H2IndexStorage {
  private val log: Logger = Logger.getLogger(classOf[H2IndexStorage])
}

class H2IndexStorage extends IndexStorage {

  private val _tableName = "DATA_INDEX"

  private var _configRoot: String = null
  private var _nodeIds: Set[Int] = null
  private var _cp: JdbcConnectionPool = null
  private var _tableColumns = new HashSet[String] with SynchronizedSet[String]

  private def getDbFile: String = "%s%sindex".format(_configRoot, File.separator)
  private def createConnectionString: String = "jdbc:h2:%s".format(getDbFile)
  private def getDbConnection: Connection = _cp.getConnection

  private def getReadOnlyDbConnection: Connection = {
    val conn: Connection = getDbConnection
    conn.setReadOnly(true)
    conn
  }

  def init(configRoot: String, nodeIds : Set[Int]) {
    _configRoot = configRoot
    _nodeIds = nodeIds

    Class.forName("org.h2.Driver")
    _cp = JdbcConnectionPool.create(createConnectionString, "sa", "sa")
    val file: File = new File(getDbFile + ".h2.db")
    if (!file.exists) bootstrapDb

    loadColumnNames()
  }

  def loadColumnNames() {
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

  private def filterColMap(colMap: Map[String, AnyRef]) : Map[String, AnyRef] = {
    colMap
  }

  private def createColumn(db: Connection, tableName: String, colName: String, colVal: AnyRef) {
    _tableColumns.synchronized {
      if (!_tableColumns.contains(colName)) {
        var statement: PreparedStatement = null
        try {
          var colType: String = null

          if (colVal.isInstanceOf[String]) {
            colType = "VARCHAR"
          }
          else if (colVal.isInstanceOf[Long]) {
            colType = "LONG"
          }
          else if (colVal.isInstanceOf[Int]) {
            colType = "INTEGER"
          }
          else {
            throw new IllegalArgumentException("unknown obj type: " + colVal.getClass.toString)
          }

          var sql = "ALTER TABLE %s ADD %s %s".format(tableName, colName, colType)

          statement = db.prepareStatement(sql)
          statement.execute()

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
  }

  private def add(db: Connection, tableName:String, record: Record)
  {
    var statement: PreparedStatement = null
    try {
      val colMap = filterColMap(record.colMap)
      val cols = colMap.keySet
      if (cols.size == 0) throw new IllegalArgumentException("filtered colMap has no data to index")

      val sql = "MERGE INTO %s (HASH,RAWDATA,%s) VALUES (?,?,%s);".format(
        tableName,
        cols.mkString(","),
        List.fill(cols.size)("?").mkString(","))

      colMap.keySet.foreach{ colName => {
        val upColName = colName.toUpperCase
        if (!_tableColumns.contains(upColName)) createColumn(db, tableName, upColName, colMap.get(colName).get)
      }}

      statement = db.prepareStatement(sql)
      bind(statement, 1, record.id)
      bind(statement, 2, record.rawData.toString)

      var idx = 3
      colMap.keySet.foreach{ colName => {
        bind(statement, idx, colMap.get(colName).get)
        idx += 1
      }}

      statement.execute
    }
    catch {
      case e: Exception => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(statement)
    }
  }

  private def remove(db: Connection, hash: String) {
    var statement: PreparedStatement = null
    try {
      statement = db.prepareStatement("DELETE FROM %s WHERE HASH = ?;".format(_tableName))
      bind(statement, 1, hash)
      statement.execute
    }
    catch {
      case e: Exception => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(statement)
    }
  }

  private def bind(statement: PreparedStatement, idx: Int, obj: AnyRef) {
    if (obj.isInstanceOf[String]) {
      statement.setString(idx, obj.asInstanceOf[String])
    }
    else if (obj.isInstanceOf[Long]) {
      statement.setLong(idx, obj.asInstanceOf[Long])
    }
    else if (obj.isInstanceOf[Int]) {
      statement.setInt(idx, obj.asInstanceOf[Int])
    }
    else {
      throw new IllegalArgumentException("unknown obj type: " + obj.toString)
    }
  }

  def add(datum: Record) {
    if (datum == null) return
    var db: Connection = null
    try {
      db = getDbConnection
      add(db, _tableName, datum)
    }
    catch {
      case e: JSONException => {
        H2IndexStorage.log.error(e)
      }
      case e: SQLException => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(db)
    }
  }

  def addAll(data: List[Record]) {
    if (data == null) return
    var db: Connection = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      data.foreach(add(db,_tableName,_))
      db.commit
    }
    catch {
      case e: JSONException => {
        H2IndexStorage.log.error(e)
      }
      case e: SQLException => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(db)
    }
  }

  def remove(id: String) {
    var db: Connection = null
    try {
      db = getDbConnection
      db.setAutoCommit(false)
      remove(db, id)
      db.commit
    }
    catch {
      case e: JSONException => {
        e.printStackTrace
        H2IndexStorage.log.error(e)
      }
      case e: SQLException => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(db)
    }
  }

  def query(filter: JSONObject) : List[Record] = {
    null
  }

  def query(sql: String): List[Record] = {
    val results = new ListBuffer[Record]

    var db: Connection = null
    var statement: PreparedStatement = null
    try {
      db = getReadOnlyDbConnection
      statement = db.prepareStatement(sql)
      val rs: ResultSet = statement.executeQuery
      while (rs.next) {
        results.append(new Record(rs.getString("HASH"), null, new JSONObject(rs.getString("RAWDATA"))))
      }
    }
    catch {
      case e: JSONException => {
        e.printStackTrace
        H2IndexStorage.log.error(e)
      }
      case e: SQLException => {
        H2IndexStorage.log.error(e)
      }
    }
    finally {
      SqlUtil.SafeClose(statement)
      SqlUtil.SafeClose(db)
    }
    results.toList
  }
}