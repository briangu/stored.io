package io.stored.server

import _root_.io.viper.core.server.router._
import common._
import ext.storage.H2IndexStorage
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.json.{JSONArray, JSONObject}
import collection.immutable._
import java.io.StringReader
import net.sf.jsqlparser.statement.select.Select
import sql.SqlRequestProcessor
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.JSQLParserException


class Node(val localNode : IndexStorage, val projections: ProjectionsConfig) {}

object Node {

  var node: Node = null

  def indexRecords(projection: Projection, is: IndexStorage, nodeIdMap: Map[Int, Set[String]], recordMap: Map[String, Record]) {
    val ids = is.addAll(projection, nodeIdMap, recordMap)
    if (ids != null) println("indexed: " + ids.mkString(","))
  }

  def indexRecord(projection: Projection, is: IndexStorage, nodeIds: Set[Int], record: Record) = {
    val id = is.add(projection, nodeIds, record)
    if (id != null) println("indexed: " + id)
  }

  def initialize(localhost: String, storagePath: String, nodesConfigFile: String, projectionsConfigFile: String) {
    H2IndexStorage.init
    val localNode = H2IndexStorage.create(storagePath)
    val projections = ProjectionsConfig.create(localhost, localNode, nodesConfigFile, projectionsConfigFile)
    node = new Node(localNode, projections)
  }

  def queryNode(projection: Projection, is: IndexStorage, nodeIds: Set[Int], sql: String) : List[Record] = {
    is.query(projection, nodeIds, sql)
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
      if (src.has(key)) {
        dst.put(key, src.get(key))
      }
    } else {
      val dstChild = new JSONObject()
      if (src.has(key)) {
        dst.put(key, dstChild)
        copyJsonObjectPath(src.getJSONObject(key), dstChild, path.slice(1, path.size))
      }
    }
  }

  def applySelectItems(selectedItems: List[String], records: List[Record]) : List[Record] = {
    if (selectedItems == null || selectedItems.size == 0 || selectedItems(0).equals("*")) return records

    val filteredRecords = records.map { record =>
      val dst = new JSONObject()
      selectedItems.foreach(rawPath => copyJsonObjectPath(record.rawData, dst, rawPath.split("__").toList))
      new Record(record.id, null, dst)
    }

    // TODO: can this be rolled into previous map operation?
    filteredRecords.filter(_.rawData.length() > 0)
  }

  def getRequestedProjection(args: java.util.Map[String, String], defaultProjName: String) : Projection = {
    if (args.containsKey("projection") && (node.projections.hasProjection(args.get("projection")))) {
      node.projections.getProjection(args.get("projection"))
    } else {
      node.projections.getProjection(defaultProjName)
    }
  }

  def getRequestedProjection(args: java.util.Map[String, String]) : Projection = {
    getRequestedProjection(args, node.projections.getDefaultProjection.name)
  }

  def getJsonRecords(args: java.util.Map[String, String]) : JSONArray = {
    if (args.containsKey("records")) {
      new JSONArray(args.get("records"))
    } else {
      val arr = new JSONArray()
      arr.put(new JSONObject(args.get("record")))
      arr
    }
  }

  // 7080 src/main/resources/db src/main/resources/nodes.json src/main/resources/projections.json
  def main(args: Array[String]) {

    val localPort = args(0).toInt
    val storagePath = args(1)
    val nodesConfigFile = args(2)
    val projectionsConfigFile = args(3)

    println("booting...")
    initialize("http://localhost:%d".format(localPort), storagePath, nodesConfigFile, projectionsConfigFile)
    println("ready!")

    // transform map into flat namespace map
    // perform hyperspace hashing on named fields using proj reference
    // store Record into table
    //  ensure all flattened field names exist using db proj reference
    //
    // determine which shard node to use from hash coords
    //  in the localhost case, we hold all shards on one Node
    //  in the multi-node case, we'd have to forward the request to teh appropriate node based on hashCoords
    //
    NestServer.run(localPort, new RestServer {
      def addRoutes {

        post("/records", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            if (!args.containsKey("record") && !args.containsKey("records")) {
              return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            }

            val projection = getRequestedProjection(args)
            val jsonRecords = getJsonRecords(args)

            val recordMap : Map[String, Record] = Map() ++ (0 until jsonRecords.length).par.map { i =>
              val record = Record.create(jsonRecords.getJSONObject(i))
              val intersection = record.colMap.keySet.intersect(projection.getFields)
              if (intersection.size != projection.getFields.size) {
                return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
              }
              (record.id, record)
            }

            val nodeIdMap = new collection.mutable.HashMap[Int, Set[String]]
            recordMap.values.foreach{ record =>
              projection.getNodeIds(record).foreach{ id =>
                if (nodeIdMap.contains(id)) {
                  nodeIdMap.get(id).get ++ record.id
                } else {
                  nodeIdMap.put(id, Set[String](record.id))
                }
              }
            }

            val nodeStoreMap = projection.getNodeStores(nodeIdMap.keySet.toSet)
            nodeStoreMap.keySet.foreach{ node =>
              val storeNodeIds = nodeStoreMap.get(node).get
              val storeNodeIdMap = nodeIdMap.filterKeys(storeNodeIds)
              val storeNodeRecordIds = storeNodeIdMap.values.foldLeft(Set[String]())(_ union _)
              val storeRecords = recordMap.filterKeys(storeNodeRecordIds)
              indexRecords(projection, node, storeNodeIdMap.toMap, storeRecords.toMap)
            }

            JsonUtil.jsonResponse("id", JsonUtil.toJsonArray(recordMap.keySet.toList))
          }
        })

        post("/records/queries", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            try {
              if (!args.containsKey("sql")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)

              val sql = args.get("sql")
              val (sqlProjectionName, nodeSql, selectedItems, whereItems) = processSqlRequest(sql)
              val projection = getRequestedProjection(args, sqlProjectionName)
              if (projection == null) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
              val nodeIds = if (args.containsKey("nodeIds")) JsonUtil.intSetFromJsonArray(args.get("nodeIds")) else projection.getNodeIds(whereItems)
              val nodeMap = projection.getNodeStores(nodeIds)

              println("nodeSql: " + nodeSql)

              // TODO: we need to apply a final-sql that is generated from the original sql in some cases
              //       when COUNT is specified in select
              //       possibly when group-by is specified
              //       the processSqlRequest should produce a finalSql, which may == nodeSql
              val results : List[Record] = if (nodeMap.keySet.size == 1 /* && nodeSql != finalSql */) {
                queryNode(projection, projection.getNodeIndexStorage(nodeIds.toList(0)), nodeIds, nodeSql)
              } else {
                var mergedResults : List[Record] = null
                val mergeDb = H2IndexStorage.createInMemoryDb
                try {
                  nodeMap.keySet.foreach{node =>
                    val nodeMapIds = nodeMap.get(node).get
                    mergeDb.addAll(projection, nodeMapIds, queryNode(projection, node, nodeMapIds, nodeSql))
                  }
                  mergedResults = mergeDb.query(projection, nodeIds, nodeSql) // TODO: null may be better for nodeIds
                } finally {
                  mergeDb.shutdown()
                }
                mergedResults
              }
              val filteredResults = applySelectItems(selectedItems, results)

              JsonUtil.jsonResponse("elements", JsonUtil.toJsonArray(filteredResults, { record: Record => record.rawData }))
            } catch {
              case e: JSQLParserException => return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            }
          }
        })
    }})
  }
}
