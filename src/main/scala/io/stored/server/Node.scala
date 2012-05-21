package io.stored.server

import _root_.io.viper.core.server.router._
import common.{Schema, IndexStorage, Record}
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import java.security.MessageDigest
import io.stored.common.FileUtil
import org.json.{JSONArray, JSONObject}
import io.stored.server.ext.storage.H2IndexStorage
import collection.immutable._
import java.io.StringReader
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.{PlainSelect, Union, SelectVisitor, Select}
import java.util.Arrays
import collection.mutable.{SynchronizedBuffer, ArrayBuffer, ListBuffer, SynchronizedMap}
import net.sf.jsqlparser.expression.ExpressionVisitor
import sql.SqlRequestProcessor


class Node(val localhost: String, val ids: Set[Int], val schema: Schema) {

  val storage = new collection.mutable.HashMap[Int, IndexStorage] with SynchronizedMap[Int, IndexStorage]

}

object Node {

  var node: Node = null

  def getNodeId(hashCoords: Map[String, Array[Byte]], schema: Schema) : Int = {
    0
  }

  def getTargetNodeId(data: Map[String, AnyRef], schema: Schema) : Int = {
//    val hashCoords = hashSchemaFields(data, schema)
    0
  }

  def getTargetNodeIds(sql: String) : Set[Int] = {
    Set(0)
  }

  def getTargetHosts(nodeIds: Set[Int]) : Map[String, Set[Int]] = {
    Map((node.localhost, nodeIds))
  }

  def hashSchemaFields(data: Map[String, AnyRef], schema: Schema) : Map[String, Array[Byte]] = {
    null
  }

  def indexRecord(nodeId: Int, record: Record) {
    if (node.ids.contains(nodeId)) {
      node.storage.get(nodeId).get.add(record)
    } else {
      indexRecord(findNodeHost(nodeId), record)
    }
  }

  def indexRecord(host: String, record: Record) {

  }

  def indexRecord(record: Record) {
  }

  def findNodeHost(nodeId: Int) : String = {
    node.localhost
  }

  def determineNodeIds(dimensions: Int) : Set[Int] = {
    val ids = new collection.mutable.HashSet[Int]
    for (x <- 0 until math.pow(2, dimensions).toInt) ids.add(x)
    ids.toSet
  }

  def md5Hash(Record: Array[Byte]) : BigInt = {
    val m = MessageDigest.getInstance("MD5");
    m.reset();
    m.update(Record);
    val digest = m.digest();
    BigInt.apply(digest)
  }

  def getNBits(bi: BigInt, dim: Int) : Int = {
     (bi & (math.pow(2,dim)-1).toInt).intValue()
  }

  def getNBits(data: Array[Byte], dim: Int) : Int = {
    getNBits(BigInt.apply(data), dim)
  }

  def getDataHash(data: String) : Array[Byte] = {
    null
  }

  def storeData(nodeId: Int, data: String) {

  }

  def getData(nodeId: Int, hash: String) : String = {
    null
  }

  def initializeDb(storagePath: String, nodeIds: Array[Int]) {

  }

  def initialize(localhost: String, schemaFile: String, storagePath: String) {
    val schema = Schema.create(FileUtil.readJson(schemaFile))
    node = new Node(localhost, determineNodeIds(schema.dimensions), schema)
    val storage = H2IndexStorage.create(storagePath)
    node.ids.par.foreach(id => node.storage.put(id, storage))
  }

  def queryHost(host: String, nodeIds: Set[Int], sql: String) : Map[Int, List[Record]] = {
    val resultMap = new collection.mutable.HashMap[Int, List[Record]] with SynchronizedMap[Int, List[Record]]
    if (host.equals(node.localhost)) {
      nodeIds.par.foreach(id => resultMap.put(id, node.storage.get(id).get.query(sql)))
    } else {
      throw new RuntimeException("not yet supported")
    }
    resultMap.toMap
  }

  def processSqlRequest(sql: String) : (String, List[String], Map[String, List[AnyRef]]) = {
    val pm = new CCJSqlParserManager();

    val statement = pm.parse(new StringReader(sql));

    if (!statement.isInstanceOf[Select]) throw new IllegalArgumentException("sql is not a select statement")

    val selectStatement = statement.asInstanceOf[Select];

    val sqlRequestProcessor = new SqlRequestProcessor
    selectStatement.getSelectBody.accept(sqlRequestProcessor)

    (statement.toString, sqlRequestProcessor.selectItems, sqlRequestProcessor.whereItems.toMap)
  }

  def copyJsonObjectPath(src: JSONObject, dst: JSONObject, path: List[String]) {
    if (path == null || path.size == 0) return

    val key = path(0)
    if (path.size == 1) {
      dst.put(key, src.get(key))
    } else {
      val dstChild = new JSONObject()
      dst.put(key, dstChild)
      copyJsonObjectPath(src.getJSONObject(key), dstChild, path.slice(1, path.size))
    }
  }

  def applySelectItems(selectedItems: List[String], records: List[Record]) : List[Record] = {
    if (selectedItems == null || selectedItems.size == 0 || selectedItems(0).equals("*")) {
      return records
    }

    val newRecords = new ListBuffer[Record]

    records.foreach( record => {
      val dst = new JSONObject()
      selectedItems.foreach( rawPath => {
        copyJsonObjectPath(record.rawData, dst, rawPath.split("_").toList)
        newRecords.append(new Record(record.id, null, dst))
      })
    })

    newRecords.toList
  }

  def main(args: Array[String]) {

    val schemaFile = args(0)
    val storagePath = args(1)

    processSqlRequest("select manufacturer from data_index where color='red' and year in (1997,1998)")

    initialize("http://localhost:8080", schemaFile, storagePath)

    NestServer.run(8080, new RestServer {
      def addRoutes {
        // transform map into flat namespace map
        // perform hyperspace hashing on named fields using schema reference
        // store Record into table
        //  ensure all flattened field names exist using db schema reference
        //
        // determine which shard node to use from hash coords
        //  in the localhost case, we hold all shards on one Node
        //  in the multi-node case, we'd have to forward the request to teh appropriate node based on hashCoords
        //
        post("/records", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            if (!args.containsKey("record")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val record = Record.create(args.get("record"))
            val nodeId = getTargetNodeId(record.colMap, node.schema)
            indexRecord(nodeId, record)
            val response = new JSONObject();
            response.put("id", record.id)
            new JsonResponse(response)
          }
        })

        post("/records/queries", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            if (!args.containsKey("sql")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val sql = args.get("sql")

            val (nodeSql, selectedItems, whereItems) = processSqlRequest(sql)

            val nodeIds = getTargetNodeIds(sql)
            val hostsMap = getTargetHosts(nodeIds)

            // combine results and TODO: apply original sql
            val results = new ArrayBuffer[Record] with SynchronizedBuffer[Record]
            hostsMap.keySet.par.foreach(host => {
              val hostResults = queryHost(host, hostsMap.get(host).get, nodeSql)
              hostResults.keys.par.foreach(id => results.appendAll(hostResults.get(id).get))
            })

            val filteredResults = applySelectItems(selectedItems, results.toList)

            val jsonResponse = new JSONObject()
            val elements = new JSONArray()
            filteredResults.foreach{ record => { elements.put( record.rawData) }}
            jsonResponse.put("elements", elements)
            new JsonResponse(jsonResponse)
          }
        })

        post("/records/queries/nodes/$node", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            if (!args.containsKey("sql")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            if (!args.containsKey("node")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val sql = args.get("sql")
            val nodeId = args.get("node").toInt

            if (!node.storage.contains(nodeId)) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)

            val results = node.storage.get(nodeId).get.query(sql)

            val jsonResponse = new JSONObject()
            val elements = new JSONArray()
            results.foreach{ record => { elements.put( record.rawData) }}
            jsonResponse.put("elements", elements)
            new JsonResponse(jsonResponse)
          }
        })
    }})
  }
}
