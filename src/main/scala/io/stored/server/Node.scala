package io.stored.server

import _root_.io.viper.core.server.router._
import common.{ProjectionField, Projection, IndexStorage, Record}
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import io.stored.common.FileUtil
import org.json.{JSONArray, JSONObject}
import io.stored.server.ext.storage.H2IndexStorage
import collection.immutable._
import java.io.StringReader
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.Select
import collection.mutable.{SynchronizedBuffer, ArrayBuffer, ListBuffer, SynchronizedMap}
import sql.SqlRequestProcessor


class Node(val localhost: String, val allNodeIds: Set[Int], val ids: Set[Int], val projection: Projection) {

  val storage = new collection.mutable.HashMap[Int, IndexStorage] with SynchronizedMap[Int, IndexStorage]

}

object Node {

  var node: Node = null

  def getNodeIds(projection: Projection, hashCoords: Map[String, List[BigInt]]) : Set[Int] = {
    if (hashCoords == null || hashCoords.size == 0) return node.allNodeIds

    val intersection = hashCoords.keySet.intersect(projection.getFields)
    if (intersection.size == 0) return node.allNodeIds

    // fill each project field with all appropriate values
    val fieldMap = new collection.mutable.HashMap[String, List[BigInt]]
    projection.getFields.foreach{ key =>
      if (hashCoords.contains(key)) {
        fieldMap.put(key, hashCoords.get(key).get)
      } else {
        fieldMap.put(key, genFieldValues(projection.getFieldValue(key).bitWeight))
      }
    }

    val fieldList = projection.getFields.toList

    // generate all possible nodeIds using the fieldMap
    val nodeIds = new collection.mutable.HashSet[Int]
    genNodeIds(fieldList, projection, fieldMap.toMap, nodeIds, 0)
    nodeIds.toSet
  }

  def genFieldValues(bitwidth: Int) : List[BigInt] = {
    val ids = new collection.mutable.HashSet[BigInt]
    for (x <- 0 until math.pow(2, bitwidth).toInt) ids.add(BigInt.apply(x))
    ids.toList
  }

  def genNodeIds(fields: List[String], proj: Projection, fieldMap: Map[String, List[BigInt]], nodeIds: collection.mutable.HashSet[Int], nodeIdPrefix: Int) {

    if (fields.size == 0) {
      nodeIds.add(nodeIdPrefix)
      return
    }

    val bitWidth = proj.getFieldValue(fields(0)).bitWeight
    val shifted = BigInt.apply(nodeIdPrefix << bitWidth)
    val mask = (math.pow(2, bitWidth).toInt - 1)

    fieldMap.get(fields(0)).get.foreach{ bitVal =>
      val nodeId = shifted | (bitVal & mask)
      genNodeIds(fields.slice(1, fields.size), proj, fieldMap, nodeIds, nodeId.toInt)
    }
  }

  def getNodeHosts(nodeIds: Set[Int]) : Map[String, Set[Int]] = {
    Map((node.localhost, nodeIds))
  }

  // TODO: support multiple values for data map
  def hashProjectionFields(projection: Projection, data: Map[String, AnyRef]) : Map[String, List[BigInt]] = {
    val result = new collection.mutable.HashMap[String, List[BigInt]] with SynchronizedMap[String, List[BigInt]]
    projection.getFields.par.foreach{ key =>
      if (data.contains(key)) {
        val dataVal = data.get(key).get

        if (dataVal.isInstanceOf[Long]) {
          result.put(key, List(ProjectionField.md5Hash(dataVal.asInstanceOf[Long])))
        } else if (dataVal.isInstanceOf[java.lang.Integer]) {
          result.put(key, List(ProjectionField.md5Hash(dataVal.asInstanceOf[java.lang.Integer])))
        } else if (dataVal.isInstanceOf[String]) {
          result.put(key, List(ProjectionField.md5Hash(dataVal.asInstanceOf[String])))
        } else if (dataVal.isInstanceOf[Double]) {
          result.put(key, List(ProjectionField.md5Hash(dataVal.asInstanceOf[Double])))
        } else {
          throw new RuntimeException("unknown field type: " + dataVal.getClass)
        }
      }
    }
    result.toMap
  }

  def indexRecord(nodeIds: Set[Int], record: Record) {
    nodeIds.par.foreach{ nodeId =>
      if (node.ids.contains(nodeId)) {
        node.storage.get(nodeId).get.add(record)
      } else {
        indexRecord(findNodeHost(nodeId), record)
      }
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

  def determineAllNodeIds(dimensions: Int) : Set[Int] = {
    val ids = new collection.mutable.HashSet[Int]
    for (x <- 0 until math.pow(2, dimensions).toInt) ids.add(x)
    ids.toSet
  }

  def initialize(localhost: String, schemaFile: String, storagePath: String) {
    val schema = Projection.create(FileUtil.readJson(schemaFile))
    node = new Node(localhost, determineAllNodeIds(schema.dimensions), determineNodeIds(schema.dimensions), schema)
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

  def processSqlRequest(sql: String) : (String, List[String], Map[String, List[BigInt]]) = {
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

    processSqlRequest("select * from data_index")
    processSqlRequest("select * from data_index where color='red' and year in (1997,1998)")
    processSqlRequest("select manufacturer from data_index where color='red' and year in (1997,1998)")

    initialize("http://localhost:8080", schemaFile, storagePath)

    NestServer.run(8080, new RestServer {
      def addRoutes {
        // transform map into flat namespace map
        // perform hyperspace hashing on named fields using proj reference
        // store Record into table
        //  ensure all flattened field names exist using db proj reference
        //
        // determine which shard node to use from hash coords
        //  in the localhost case, we hold all shards on one Node
        //  in the multi-node case, we'd have to forward the request to teh appropriate node based on hashCoords
        //
        post("/records", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            if (!args.containsKey("record")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val record = Record.create(args.get("record"))

            val intersection = record.colMap.keySet.intersect(node.projection.getFields)
            if (intersection.size != node.projection.getFields.size) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)

            val hashCoords = hashProjectionFields(node.projection, record.colMap)

            val nodeIds = getNodeIds(node.projection, hashCoords)
            indexRecord(nodeIds, record)
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

            val nodeIds = getNodeIds(node.projection, whereItems)
            val hostsMap = getNodeHosts(nodeIds)

            // combine results and TODO: apply original sql
            val results = new collection.mutable.HashMap[String, Record] with SynchronizedMap[String, Record]
            hostsMap.keySet.par.foreach(host => {
              val hostResults = queryHost(host, hostsMap.get(host).get, nodeSql)
              hostResults.keys.par.foreach{id =>
                hostResults.get(id).get.foreach{ record =>
                  results.put(record.id, record)
                }
              }
            })

            val filteredResults = applySelectItems(selectedItems, results.values.toList)

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
