package io.stored.server

import _root_.io.viper.core.server.router._
import common.{Record, IndexStorage, Schema}
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import java.security.MessageDigest
import collection.mutable.{HashSet}
import io.stored.common.FileUtil
import io.stored.server.storage.H2IndexStorage
import org.json.{JSONArray, JSONObject}


class Node(val localhost: String, val ids: Set[Int], val schema: Schema) {

  val storage : IndexStorage = new H2IndexStorage

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

  def hashSchemaFields(data: Map[String, AnyRef], schema: Schema) : Map[String, Array[Byte]] = {
    null
  }

  def indexRecord(nodeId: Int, record: Record) {
    if (node.ids.contains(nodeId)) {
      indexRecord(record)
    } else {
      indexRecord(findNodeHost(nodeId), record)
    }
  }

  def indexRecord(host: String, record: Record) {

  }

  def indexRecord(record: Record) {
    node.storage.add(record)
  }

  def findNodeHost(nodeId: Int) : String = {
    node.localhost
  }

  def determineNodeIds(dimensions: Int) : Set[Int] = {
    val ids = new HashSet[Int]
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
    node.storage.init(storagePath, node.ids)
  }

  def main(args: Array[String]) {

    val schemaFile = args(0)
    val storagePath = args(1)

    initialize("http://localhost:8080", schemaFile, storagePath)

    NestServer.run(8080, new RestServer {
      def addRoutes {
/*
        post("/schema", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            if (!args.containsKey("schema")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val schema = Schema.create(new JSONObject(args.get("schema")))
            node = new Node(determineNodeIds(schema.dimensions), schema)
            new StatusResponse(HttpResponseStatus.OK)
          }
        })

        get("/schema", new RouteHandler {
          def exec(args: java.util.Map[String, String]) = new JsonResponse(new JSONObject())
        })
*/

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
            val results = node.storage.query(sql)
            val jsonResponse = new JSONObject()
            val elements = new JSONArray()
            results.foreach{ record => { elements.put( record.rawData) }}
            jsonResponse.put("elements", elements)
            new JsonResponse(jsonResponse)
          }
        })

/*
        post("/data", new RouteHandler {
          def exec(args: java.util.Map[String, String]): RouteResponse = {
            if (!args.containsKey("data")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val rawData = args.get("data")
            val hash = getDataHash(rawData)
            val nodeId = getNBits(hash, node.schema.dimensions)
            indexRecord(nodeId, rawData)
            new StatusResponse(HttpResponseStatus.OK)
          }
        })

        get("/data/$hash", new RouteHandler {
          def exec(args: java.util.Map[String, String]) = {
            val hash = args.get("hash")
            val nodeId = getNBits(HexBin.decode(hash), node.schema.dimensions)
            val data = getData(nodeId, hash)
            new JsonResponse(new JSONObject(data))
          }
        })
*/
      }
    })
  }
}


