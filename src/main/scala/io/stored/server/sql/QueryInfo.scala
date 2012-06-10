package io.stored.server.sql

import net.sf.jsqlparser.parser.CCJSqlParserManager
import java.io.StringReader
import net.sf.jsqlparser.statement.select.Select

object QueryInfo {
  def create(sql: String) : QueryInfo = {
    val pm = new CCJSqlParserManager
    val statement = pm.parse(new StringReader(sql))
    if (!statement.isInstanceOf[Select]) throw new IllegalArgumentException("sql is not a select statement")
    val selectStatement = statement.asInstanceOf[Select]
    val sp = new SqlRequestProcessor
    selectStatement.getSelectBody.accept(sp)

    new QueryInfo(
      sp.projectionName,
      statement.toString,
      statement.toString,
      sp.selectItems,
      sp.whereItems.toMap)
  }
}

class QueryInfo(val projectionName: String,
                val nodeSql: String,
                val finalSql: String,
                val selectedItems: List[String],
                val whereItems: Map[String, List[BigInt]])
{}
