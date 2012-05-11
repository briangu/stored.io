package io.stored.server

import _root_.io.viper.core.server.router._
import common.Schema
import io.viper.common.{NestServer, RestServer}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.json.JSONObject
import java.util.Map


class Node(schema: Schema) {

}

object Node {

  var node: Node = null

  def flatten(jsonData: JSONObject) : Map[String, String] = {
    null
  }

  def applyHSHashing(data: Map[String, String]) : Map[String, Int] = {
    null
  }

  def ensureColumnsExist(data: Map[String, String]) {

  }

  def storeData(data: Map[String, String]) {
    ensureColumnsExist(data)

  }

  def main(args: Array[String]) {
    NestServer.run(8080, new RestServer {
      def addRoutes {
        post("/schema", new RouteHandler {
          def exec(args: Map[String, String]): RouteResponse = {
            if (!args.containsKey("schema")) return new StatusResponse(HttpResponseStatus.BAD_REQUEST)
            val schema = Schema.create(new JSONObject(args.get("schema")))
            node = new Node(schema)
            new StatusResponse(HttpResponseStatus.OK)
          }
        })

        // transform map into flat namespace map
        // perform hyperspace hashing on named fields using schema reference
        // store datum into table
        //  ensure all flattened field names exist using db schema reference
        post("/data", new RouteHandler {
          def exec(args: Map[String, String]): RouteResponse = {
            val data = flatten(new JSONObject(args.get("schema")))
            val hashCoords = applyHSHashing(data)
            // determine which shard node to use from hash coords
            //  in the localhost case, we hold all shards on one Node
            //  in the multi-node case, we'd have to forward the request to teh appropriate node based on hashCoords
            storeData(data)
            new StatusResponse(HttpResponseStatus.OK)
          }
        })
      }
    })
  }
}


