package io.stored.server

import _root_.io.viper.core.server.router._
import common.Schema
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.json.JSONObject
import java.util.Map
import collection.mutable.HashSet


class Node(val ids: Set[Int], val schema: Schema) {

}

object Node {

  var node: Node = null

  def flatten(jsonData: JSONObject) : Map[String, String] = {
    null
  }

  def getNodeId(hashCoords: Map[String, Array[Byte]], schema: Schema) : Int = {
    0
  }

  def getTargetNodeIdFromData(data: Map[String, String], schema: Schema) : Int = {
    val hashCoords = applyHSHashing(data, schema)
    0
  }

  def applyHSHashing(data: Map[String, String], schema: Schema) : Map[String, Array[Byte]] = {
    null
  }

  def ensureColumnsExist(data: Map[String, String]) {

  }

  def storeData(nodeId: Int, data: Map[String, String]) {
    if (node.ids.contains(nodeId)) {
      storeData(data)
    } else {
      storeData(findNodeHost(nodeId), data)
    }
  }

  def storeData(host: String, data: Map[String, String]) {

  }

  def storeData(data: Map[String, String]) {
    //ensureColumnsExist(data)
    // can be done implicitly as we fill out the write request
  }

  def findNodeHost(nodeId: Int) : String = {
    null
  }

  def determineNodeIds(dimensions: Int) : Set[Int] = {
    val ids = new HashSet[Int]
    // for 2^dimensions, store every node
//    for (x <- 0 .. Math.)
    ids.toSet
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

        // transform map into flat namespace map
        // perform hyperspace hashing on named fields using schema reference
        // store datum into table
        //  ensure all flattened field names exist using db schema reference
        //
        // determine which shard node to use from hash coords
        //  in the localhost case, we hold all shards on one Node
        //  in the multi-node case, we'd have to forward the request to teh appropriate node based on hashCoords
        //
        post("/data", new RouteHandler {
          def exec(args: Map[String, String]): RouteResponse = {
            val data = flatten(new JSONObject(args.get("data")))
            val nodeId = getTargetNodeIdFromData(data, node.schema)
            storeData(nodeId, data)
            new StatusResponse(HttpResponseStatus.OK)
          }
        })
      }
    })
  }
}


