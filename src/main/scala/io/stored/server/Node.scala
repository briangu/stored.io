package io.stored.server

import _root_.io.viper.core.server.router._
import common._
import ext.storage.H2IndexStorage
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.json.{JSONArray, JSONObject}
import net.sf.jsqlparser.JSQLParserException
import collection.immutable._
import sql.QueryInfo
import collection.mutable.SynchronizedSet


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
    val localNode = H2IndexStorage.create(storagePath)
    val projections = ProjectionsConfig.create(localhost, localNode, nodesConfigFile, projectionsConfigFile)
    node = new Node(localNode, projections)
  }

  def queryNode(projection: Projection, is: IndexStorage, nodeIds: Set[Int], sql: String) : List[Record] = {
    is.query(projection, nodeIds, sql)
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

              val results = if (args.containsKey("nodeIds")) {
                val nodeSql = args.get("sql")
                val projection = getRequestedProjection(args)
                if (projection == null) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
                val nodeIds = JsonUtil.intSetFromJsonArray(args.get("nodeIds"))
                val nodeMap = Map(node.localNode -> nodeIds)
                val queryInfo = new QueryInfo(projection.name, nodeSql, nodeSql, List(), Map())
                doQuery(queryInfo, projection, nodeMap, nodeIds)
              } else {
                val queryInfo = QueryInfo.create(args.get("sql"))
                val projection = getRequestedProjection(args, queryInfo.projectionName)
                if (projection == null) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
                val nodeIds = projection.getNodeIds(queryInfo.whereItems)
                val nodeMap = projection.getNodeStores(nodeIds)
                doQuery(queryInfo, projection, nodeMap, nodeIds)
              }

              JsonUtil.jsonResponse("records", JsonUtil.toJsonArray(results, { record: Record => record.rawData }))
            } catch {
              case e: JSQLParserException => return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            }
          }
        })

        def doQuery(queryInfo: QueryInfo, projection: Projection, nodeMap: Map[IndexStorage, Set[Int]], nodeIds: Set[Int]) : List[Record] = {
  //        println("nodeSql: " + queryInfo.nodeSql)

          // TODO: we need to apply a final-sql that is generated from the original sql in some cases
          //       when COUNT is specified in select
          //       possibly when group-by is specified
          //       the processSqlRequest should produce a finalSql, which may == nodeSql
          val results = if (nodeMap.keySet.size == 1 && queryInfo.nodeSql.equals(queryInfo.finalSql)) {
            queryNode(projection, projection.getNodeIndexStorage(nodeIds.toList(0)), nodeIds, queryInfo.nodeSql)
          } else {
            // TODO: this is pretty horrific...and is a consequence of the H2 db having some null pointer issues
            //       when accessed from multiple threads
            val set = new scala.collection.mutable.HashSet[Record] with SynchronizedSet[Record]
            nodeMap.keySet.par.foreach{node =>
              queryNode(projection, node, nodeMap.get(node).get, queryInfo.nodeSql).foreach(set.add)
            }
            if (queryInfo.nodeSql.equals(queryInfo.finalSql)) {
              set.toList
            } else {
              // TODO: only do this when we have some post processing predicate: ORDER BY, LIMIT, GROUP BY, COUNT, etc.
              val mergeDb = H2IndexStorage.createInMemoryDb
              try {
                mergeDb.addAll(projection, null, set.toList)
                mergeDb.query(projection, nodeIds, queryInfo.finalSql)
              } finally {
                mergeDb.shutdown()
              }
            }
          }

          applySelectItems(queryInfo.selectedItems, results)
        }
    }})
  }
}
