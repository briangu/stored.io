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
import io.stored.common.sql.TablesNamesFinder
import net.sf.jsqlparser.statement.select.{PlainSelect, Union, SelectVisitor, Select}
import java.util.Arrays
import collection.mutable.{SynchronizedBuffer, ArrayBuffer, ListBuffer, SynchronizedMap}


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

  def testjsql {
    val pm = new CCJSqlParserManager();
    var sql = "SELECT * FROM MY_TABLE1, MY_TABLE2, (SELECT * FROM MY_TABLE3) LEFT OUTER JOIN MY_TABLE4 WHERE ID = (SELECT MAX(ID) FROM MY_TABLE5) AND ID2 IN (SELECT * FROM MY_TABLE6)" ;

    sql = "select manufacturer from data_index where color = 'red' and year = 1997"

    val statement = pm.parse(new StringReader(sql));

    if (statement.isInstanceOf[Select]) {
      val selectStatement = statement.asInstanceOf[Select];

      val tablesNamesFinder = new TablesNamesFinder();
      val tableList = tablesNamesFinder.getTableList(selectStatement);
      val iter = tableList.iterator()
      while (iter.hasNext) {
        println(iter.next());
      }
    }
  }

  def createNodeSql(sql: String) : (String, List[String]) = {
    val pm = new CCJSqlParserManager();

    val statement = pm.parse(new StringReader(sql));

    if (!statement.isInstanceOf[Select]) throw new IllegalArgumentException("sql is not a select statement")

    val selectStatement = statement.asInstanceOf[Select];

    var originalSelectItems = new ListBuffer[String]

    selectStatement.getSelectBody.accept(new SelectVisitor {
      def visit(plainSelect: PlainSelect) {
        val selectItems = plainSelect.getSelectItems
        if (!(selectItems.size() == 1 && selectItems.get(0).equals("*"))) {
          for (i <- 0 until selectItems.size()) {
            originalSelectItems.append(selectItems.get(i).toString)
          }
          plainSelect.setSelectItems(Arrays.asList("*"))
        }
      }
      def visit(union: Union) {}
    })

    (statement.toString, originalSelectItems.toList)
  }

  def main(args: Array[String]) {

    val schemaFile = args(0)
    val storagePath = args(1)

    createNodeSql("select manufacturer from data_index where color='red'")

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

            val nodeIds = getTargetNodeIds(sql)
            val hostsMap = getTargetHosts(nodeIds)

            // TODO: rewrite sql for node queries, as it's almost certain what we want to sub query is not the same
            //val resultMap = new HashMap[String, Map[Int, List[Record]]] with SynchronizedMap[String, Map[Int, List[Record]]]
//            hostsMap.keySet.par.foreach(host => resultMap.put(host, queryHost(host, hostsMap.get(host).get, sql)))
            val (nodeSql, selectedItems) = createNodeSql(sql)

            // combine results and TODO: apply original sql
            val results = new ArrayBuffer[Record] with SynchronizedBuffer[Record]
            hostsMap.keySet.par.foreach(host => {
              val hostResults = queryHost(host, hostsMap.get(host).get, nodeSql)
              hostResults.keys.par.foreach(id => results.appendAll(hostResults.get(id).get))
            })

            val jsonResponse = new JSONObject()
            val elements = new JSONArray()
            results.foreach{ record => { elements.put( record.rawData) }}
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


