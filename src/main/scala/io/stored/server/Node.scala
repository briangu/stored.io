package io.stored.server

import _root_.io.viper.core.server.router._
import common.Schema
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.json.JSONObject
import java.util.Map
import collection.mutable.HashSet
import java.security.MessageDigest
import com.sun.org.apache.xerces.internal.impl.dv.util.HexBin


class Node(val ids: Set[Int], val schema: Schema) {

}

object Node {

  var node: Node = null

  def flatten(jsonData: JSONObject) : Map[String, AnyRef] = {
    null
  }

  def getNodeId(hashCoords: Map[String, Array[Byte]], schema: Schema) : Int = {
    0
  }

  def getTargetNodeIdFromData(data: Map[String, AnyRef], schema: Schema) : Int = {
    val hashCoords = hashSchemaFields(data, schema)
    0
  }

  def hashSchemaFields(data: Map[String, AnyRef], schema: Schema) : Map[String, Array[Byte]] = {
    null
  }

  def ensureColumnsExist(data: Map[String, String]) {

  }

  def indexData(nodeId: Int, data: Map[String, AnyRef], rawData: String) {
    if (node.ids.contains(nodeId)) {
      storeData(data, rawData)
    } else {
      storeData(findNodeHost(nodeId), data, rawData)
    }
  }

  def storeData(host: String, data: Map[String, AnyRef], rawData: String) {

  }

  def storeData(data: Map[String, AnyRef], rawData: String) {
    //ensureColumnsExist(data)
    // can be done implicitly as we fill out the write request
  }

  def findNodeHost(nodeId: Int) : String = {
    null
  }

  def determineNodeIds(dimensions: Int) : Set[Int] = {
    val ids = new HashSet[Int]
    for (x <- 0 until math.pow(2, dimensions)) ids.add(x)
    ids.toSet
  }

  def md5Hash(datum: Array[Byte]) : BigInt = {
    val m = MessageDigest.getInstance("MD5");
    m.reset();
    m.update(datum);
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

  def main(args: Array[String]) {
    NestServer.run(8080, new RestServer {
      def addRoutes {
        post("/schema", new RouteHandler {
          def exec(args: Map[String, String]): RouteResponse = {
            if (!args.containsKey("schema")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val schema = Schema.create(new JSONObject(args.get("schema")))
            node = new Node(determineNodeIds(schema.dimensions), schema)
            new StatusResponse(HttpResponseStatus.OK)
          }
        })

        get("/schema", new RouteHandler {
          def exec(args: Map[String, String]) = new JsonResponse(new JSONObject())
        })

        // transform map into flat namespace map
        // perform hyperspace hashing on named fields using schema reference
        // store datum into table
        //  ensure all flattened field names exist using db schema reference
        //
        // determine which shard node to use from hash coords
        //  in the localhost case, we hold all shards on one Node
        //  in the multi-node case, we'd have to forward the request to teh appropriate node based on hashCoords
        //
        post("/index", new RouteHandler {
          def exec(args: Map[String, String]): RouteResponse = {
            if (!args.containsKey("data")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val rawData = args.get("data")
            val data = flatten(new JSONObject(rawData))
            val nodeId = getTargetNodeIdFromData(data, node.schema)
            indexData(nodeId, data, rawData)
            new StatusResponse(HttpResponseStatus.OK)
          }
        })

        get("/index/$query", new RouteHandler {
          def exec(args: Map[String, String]) = new JsonResponse(new JSONObject())
        })

        post("/data", new RouteHandler {
          def exec(args: Map[String, String]): RouteResponse = {
            if (!args.containsKey("data")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val rawData = args.get("data")
            val hash = getDataHash(rawData)
            val nodeId = getNBits(hash, node.schema.dimensions)
            storeData(nodeId, rawData)
            new StatusResponse(HttpResponseStatus.OK)
          }
        })

        get("/data/$hash", new RouteHandler {
          def exec(args: Map[String, String]) = {
            val hash = args.get("hash")
            val nodeId = getNBits(HexBin.decode(hash), node.schema.dimensions)
            val data = getData(nodeId, hash)
            new JsonResponse(new JSONObject(data))
          }
        })
      }
    })
  }
}


