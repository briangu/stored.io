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


class Node(val localNode : IndexStorage, val projections: ProjectionsConfig) {

  def indexRecords(projection: Projection, is: IndexStorage, nodeIdMap: Map[Int, Set[String]], recordMap: Map[String, Record]) {
    is.addAll(projection, nodeIdMap, recordMap)
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

    val selSet = selectedItems.map(_.toUpperCase).toSet
    records.flatMap { record =>
      if (record.colMap == null || record.colMap.keySet.intersect(selSet).size > 0) {
        val dst = new JSONObject()
        selectedItems.foreach(rawPath => copyJsonObjectPath(record.rawData, dst, rawPath.split("__").toList))
        if (dst.length > 0) { List(new Record(record.id, null, dst)) } else { Nil }
      } else {
        Nil
      }
    }
  }

  def insert(projectionName: String, record: JSONObject) : String = {
    val projection = projections.getProjection(projectionName)
    if (projection == null) throw new IllegalArgumentException("using unregistered projection: " + projectionName)
    insert(projection, record)
  }

  def insert(projection: Projection, record: JSONObject) : String = {
    val records = new JSONArray()
    records.put(record)
    insert(projection, records)(0)
  }

  def insert(projectionName: String, records: JSONArray) : List[String] = {
    val projection = projections.getProjection(projectionName)
    if (projection == null) throw new IllegalArgumentException("using unregistered projection: " + projectionName)
    insert(projection, records)
  }

  def insert(projection: Projection, records: JSONArray) : List[String] = {

    val recordMap : Map[String, Record] = Map() ++ (0 until records.length).par.map { i =>
      val record = Record.create(records.getJSONObject(i))
      val intersection = record.colMap.keySet.intersect(projection.getFields)
      if (intersection.size != projection.getFields.size) {
        throw new IllegalArgumentException // TODO: add custom exceptions for stored.io so errors are sensible
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

    recordMap.keySet.toList
  }

  def select(sql: String) : List[Record] = {
    val queryInfo = QueryInfo.create(sql)
    val projection = projections.getProjection(queryInfo.projectionName)
    if (projection == null) throw new IllegalArgumentException("using unregistered projection: " + queryInfo.projectionName)
    val nodeIds = projection.getNodeIds(queryInfo.whereItems)
    val nodeMap = projection.getNodeStores(nodeIds)
    doQuery(queryInfo, projection, nodeMap, nodeIds)
  }

  private def doQuery(queryInfo: QueryInfo, projection: Projection, nodeMap: Map[IndexStorage, Set[Int]], nodeIds: Set[Int]) : List[Record] = {
    val results = if (nodeMap.keySet.size == 1 && queryInfo.nodeSql.equals(queryInfo.finalSql)) {
      projection.getNodeIndexStorage(nodeIds.toList(0)).query(projection, nodeIds, queryInfo.nodeSql)
    } else {
      val qr = nodeMap.keySet.par.flatMap(node => node.query(projection, nodeMap.get(node).get, queryInfo.nodeSql)).toList
      if (queryInfo.postSqlRequired) {
        val mergeDb = H2IndexStorage.createInMemoryDb
        try {
          mergeDb.addAll(projection, null, qr)
          mergeDb.query(projection, nodeIds, queryInfo.finalSql)
        } finally {
          mergeDb.shutdown()
        }
      } else {
        qr
      }
    }

    applySelectItems(queryInfo.selectedItems, results)
  }

  private def getRequestedProjection(args: java.util.Map[String, String], defaultProjName: String) : Projection = {
    if (args.containsKey("projection") && (projections.hasProjection(args.get("projection")))) {
      projections.getProjection(args.get("projection"))
    } else {
      projections.getProjection(defaultProjName)
    }
  }

  private def getRequestedProjection(args: java.util.Map[String, String]) : Projection = {
    getRequestedProjection(args, projections.getDefaultProjection.name)
  }
}

object Node {

  var node: Node = null

  def getJsonRecords(args: java.util.Map[String, String]) : JSONArray = {
    if (args.containsKey("records")) {
      new JSONArray(args.get("records"))
    } else {
      val arr = new JSONArray()
      arr.put(new JSONObject(args.get("record")))
      arr
    }
  }

  def createSingleNode(storagePath: String, projectionsConfigFile: String) : Node = {
    val localNode = H2IndexStorage.create(storagePath)
    val projections = ProjectionsConfig.create(localNode, projectionsConfigFile)
    new Node(localNode, projections)
  }

  def createSingleNode(storagePath: String, projectionsConfig: JSONObject) : Node = {
    val localNode = H2IndexStorage.create(storagePath)
    val projections = ProjectionsConfig.create(localNode, projectionsConfig)
    new Node(localNode, projections)
  }

  def initialize(localhost: String, storagePath: String, nodesConfigFile: String, projectionsConfigFile: String) {
    val localNode = H2IndexStorage.create(storagePath)
    val projections = ProjectionsConfig.create(localhost, localNode, nodesConfigFile, projectionsConfigFile)
    node = new Node(localNode, projections)
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

            val projection = node.getRequestedProjection(args)
            val jsonRecords = getJsonRecords(args)

            try {
              val ids = node.insert(projection, jsonRecords)
              JsonUtil.jsonResponse("id", JsonUtil.toJsonArray(ids))
            } catch {
              case e: IllegalArgumentException => {
                e.printStackTrace()
                return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
              }
            }
          }
        })

        post("/records/queries", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            try {
              if (!args.containsKey("sql")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)

              val results = if (args.containsKey("nodeIds")) {
                val nodeSql = args.get("sql")
                val projection = node.getRequestedProjection(args)
                if (projection == null) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
                val nodeIds = JsonUtil.intSetFromJsonArray(args.get("nodeIds"))
                val nodeMap = Map(node.localNode -> nodeIds)
                val queryInfo = new QueryInfo(projection.name, nodeSql, nodeSql, List(), Map(), false)
                node.doQuery(queryInfo, projection, nodeMap, nodeIds)
              } else {
                val queryInfo = QueryInfo.create(args.get("sql"))
                val projection = node.getRequestedProjection(args, queryInfo.projectionName)
                if (projection == null) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
                val nodeIds = projection.getNodeIds(queryInfo.whereItems)
                val nodeMap = projection.getNodeStores(nodeIds)
                node.doQuery(queryInfo, projection, nodeMap, nodeIds)
              }

              JsonUtil.jsonResponse("records", JsonUtil.toJsonArray(results, { record: Record => record.rawData }))
            } catch {
              case e: JSQLParserException => return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            }
          }
        })
    }})
  }
}
