package io.stored.server

import _root_.io.viper.core.server.router._
import common._
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.json.{JSONArray, JSONObject}
import io.stored.server.ext.storage.H2IndexStorage
import collection.immutable._
import java.io.StringReader
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.Select
import collection.mutable.{ListBuffer, SynchronizedMap}
import sql.SqlRequestProcessor


class Node(val localhost: String, val localNode : IndexStorage, val projections: ProjectionsConfig) {}

object Node {

  var node: Node = null

  def getNodeIds(projection: Projection, hashCoords: Map[String, List[BigInt]]) : Set[Int] = {
    if (hashCoords == null || hashCoords.size == 0) return projection.allNodeIds

    val intersection = hashCoords.keySet.intersect(projection.getFields)
    if (intersection.size == 0) return projection.allNodeIds

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

  // TODO: support multiple values for data map
  def getProjectionCoords(projection: Projection, data: Map[String, AnyRef]) : Map[String, List[BigInt]] = {
    val result = new collection.mutable.HashMap[String, List[BigInt]] with SynchronizedMap[String, List[BigInt]]
    data.keySet.par.foreach{ key =>
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
          // throw new RuntimeException("unknown field type: " + dataVal.getClass)
        }
      }
    }
    result.toMap
  }

  def indexRecord(projection: Projection, nodeIds: Set[Int], record: Record) {
    nodeIds.par.foreach{ nodeId =>
      if (projection.localNodeIds.contains(nodeId)) {
        indexRecord(record)
      } else {
        indexRecord(projection.getNodeIndexStorage(nodeId), record)
      }
    }
  }

  def indexRecord(is: IndexStorage, record: Record) {}

  def indexRecord(record: Record) = node.localNode.add(record)

  def initialize(localhost: String, storagePath: String, nodesConfigFile: String, projectionsConfigFile: String) {
    val localNode = H2IndexStorage.create(storagePath)
    val projections = ProjectionsConfig.create(localhost, localNode, nodesConfigFile, projectionsConfigFile)
    node = new Node(localhost, localNode, projections)
  }

  def queryHost(host: IndexStorage, nodeIds: Set[Int], sql: String) : Map[Int, List[Record]] = {
    val resultMap = new collection.mutable.HashMap[Int, List[Record]] with SynchronizedMap[Int, List[Record]]
    if (host == node.localNode) {
      val result = node.localNode.query(sql)
      nodeIds.par.foreach(id => resultMap.put(id, result))
    } else {
      //val result = host.query(nodeIds, sql)
      throw new RuntimeException("not yet supported")
    }
    resultMap.toMap
  }

  def processSqlRequest(sql: String) : (String, String, List[String], Map[String, List[BigInt]]) = {
    val pm = new CCJSqlParserManager
    val statement = pm.parse(new StringReader(sql))
    if (!statement.isInstanceOf[Select]) throw new IllegalArgumentException("sql is not a select statement")
    val selectStatement = statement.asInstanceOf[Select]
    val sp = new SqlRequestProcessor
    selectStatement.getSelectBody.accept(sp)
    (sp.projectionName, statement.toString, sp.selectItems, sp.whereItems.toMap)
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
        copyJsonObjectPath(record.rawData, dst, rawPath.split("__").toList)
        newRecords.append(new Record(record.id, null, dst))
      })
    })
    newRecords.toList
  }

  def jsonResponse(args: AnyRef*) : JsonResponse = {
    if (args.length % 2 != 0) throw new RuntimeException("expecting key value pairs")
    val obj = new JSONObject
    (0 until args.length by 2).foreach(i => obj.put(args(i).toString(), args(i+1)))
    new JsonResponse(obj)
  }

  def getRequestedProjection(args: java.util.Map[String, String]) : Projection = {
    if (args.containsKey("projection") && (node.projections.hasProjection(args.get("projection")))) {
      node.projections.getProjection(args.get("projection"))
    } else {
      node.projections.getDefaultProjection
    }
  }

  def main(args: Array[String]) {

    val localPort = args(0).toInt
    val storagePath = args(1)
    val nodesConfigFile = args(2)
    val projectionsConfigFile = args(3)

    initialize("http://localhost:%d".format(localPort), storagePath, nodesConfigFile, projectionsConfigFile)

    NestServer.run(localPort, new RestServer {
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
            if (!args.containsKey("record")) {
              return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            }

            val record = Record.create(args.get("record"))
            val projection = getRequestedProjection(args)

            val intersection = record.colMap.keySet.intersect(projection.getFields)
            if (intersection.size != projection.getFields.size) {
              return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            }

            val coords = getProjectionCoords(projection, record.colMap)
            val nodeIds = getNodeIds(projection, coords)
            indexRecord(projection, nodeIds, record)

            jsonResponse("id", record.id)
          }
        })

        post("/records/queries", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            if (!args.containsKey("sql")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)

            val sql = args.get("sql").replace(".", "__") // TODO correct using jsqlparser visitor
            val (projectionName, nodeSql, selectedItems, whereItems) = processSqlRequest(sql)
            if (node.projections.hasProjection(projectionName)) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val projection = node.projections.getProjection(projectionName)
            val nodeIds = getNodeIds(projection, whereItems)
            val hostsMap = projection.getNodeHosts(nodeIds)

            val results : List[Record] = if (nodeIds.size == 1) {
              queryHost(projection.getNodeIndexStorage(nodeIds.toList(0)), nodeIds, nodeSql).values.toList(0)
            } else {
              var hostResults : List[Record] = null
              val mergeDb = H2IndexStorage.createInMemoryDb
              try {
                hostsMap.keySet.par.foreach{host =>
                  val hostResults = queryHost(host, hostsMap.get(host).get, nodeSql)
                  hostResults.keys.par.foreach(id => mergeDb.addAll(hostResults.get(id).get.toList))
                  hostResults = mergeDb.query(nodeSql)
                }
              } finally {
                mergeDb.shutdown()
              }
              hostResults
            }
            val filteredResults = applySelectItems(selectedItems, results)

            val elements = new JSONArray()
            filteredResults.foreach{ record => { elements.put( record.rawData) }}
            jsonResponse("elements", elements)
          }
        })
    }})
  }
}
