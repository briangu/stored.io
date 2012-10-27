package io.stored.server.ext.storage


import com.ning.http.client.AsyncHttpClient
import collection.mutable.ListBuffer
import org.json.{JSONArray, JSONObject}
import io.stored.server.common.{JsonUtil, Projection, Record, IndexStorage}
import io.stored.common.CryptoUtil


class HttpIndexStorage(host: String) extends IndexStorage
{
  val asyncHttpClient = new AsyncHttpClient()

  def init()
  {}

  def shutdown()
  {}

  def purge()
  {}


  def query(projection: Projection, nodeIds: Set[Int], sql: String) : List[Record] = {
    val response = asyncHttpClient.
      preparePost("%s/records/queries".format(host))
      .addParameter("sql",sql)
      .addParameter("projection", projection.name)
      .addParameter("nodeIds", JsonUtil.toJsonArray(nodeIds).toString)
      .execute
      .get
    val responseBody = response.getResponseBody
    if (responseBody.startsWith("{")) {
      val jsonResponse = new JSONObject(responseBody)
      if (jsonResponse.has("records")) {
        val elements = jsonResponse.getJSONArray("records")

        val results = new ListBuffer[Record]
        (0 until elements.length()).foreach(i => results.append(Record.create(elements.getJSONObject(i))))
        results.toList
      } else {
        List()
      }
    } else {
      List()
    }
  }

  def jsonQuery(projection: Projection, nodeIds: Set[Int], sql: String) : String = {
    ""
  }

  def add(projection: Projection, nodeIds: Set[Int], record: Record) : String = {
    val response = asyncHttpClient
      .preparePost("%s/records".format(host))
      .addParameter("projection", projection.name)
      .addParameter("record", record.rawData.toString)
      .execute
      .get
    val responseBody = response.getResponseBody
    if (responseBody.startsWith("{")) {
      val jsonResponse = new JSONObject(responseBody)
      if (jsonResponse.has("id")) {
        jsonResponse.getString("id")
      } else {
        null
      }
    } else {
      null
    }
  }

  def addAll(projection: Projection, nodeIdMap: Map[Int, Set[String]], recordMap: Map[String, Record]) : List[String] = {
    try {
      val response = asyncHttpClient
        .preparePost("%s/records".format(host))
        .addParameter("projection", projection.name)
        .addParameter("records", JsonUtil.toJsonArray(recordMap.values.toList, {r:Record => r.rawData}).toString)
        .execute
        .get

      val responseBody = response.getResponseBody
      if (responseBody.startsWith("{")) {
        val jsonResponse = new JSONObject(responseBody)
        if (jsonResponse.has("id")) {
          val arr = new JSONArray(jsonResponse.getString("id"))
          val idSet = (0 until arr.length()).map(i => arr.getString(i)).toSet
          if (!idSet.equals(recordMap.keySet)) {
            println("stored records have different hashes!")
            println("expecting: " + recordMap.keySet.mkString(","))
            println("received: " + idSet.mkString(","))
            throw new RuntimeException("stored records have different hashes!")
          }
          idSet.toList
        } else {
          null
        }
      } else {
        null
      }
    } catch {
      case e: Exception => {
        e.printStackTrace
        null
      }
    }
  }

  def remove(projection: Projection, nodeIds: Set[Int], ids: List[String]) {}
}

object HttpIndexStorage
{
  def create(host: String) : HttpIndexStorage = {
    val indexStorage = new HttpIndexStorage(host)
    indexStorage.init
    indexStorage
  }
}