package io.stored.server.ext.storage


import com.ning.http.client.AsyncHttpClient
import collection.mutable.ListBuffer
import org.json.{JSONArray, JSONObject}
import io.stored.server.common.{JsonUtil, Projection, Record, IndexStorage}


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
      .addParameter("nodeIds", JsonUtil.toJsonArray(nodeIds).toString)
      .execute
      .get
    val responseBody = response.getResponseBody
    if (responseBody.startsWith("{")) {
      val jsonResponse = new JSONObject(responseBody)
      if (jsonResponse.has("elements")) {
        val elements = jsonResponse.getJSONArray("elements")

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

  def add(projection: Projection, nodeIds: Set[Int], record: Record) : String = {
    val response = asyncHttpClient
      .preparePost("%s/records".format(host))
      .addParameter("record", record.rawData.toString)
      .addParameter("projection", projection.name)
      .execute
      .get
    val responseBody = response.getResponseBody
    if (responseBody.startsWith("{")) {
      val jsonResponse = new JSONObject()
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
    val response = asyncHttpClient
      .preparePost("%s/records".format(host))
      .addParameter("records", JsonUtil.toJsonArray(recordMap.values.toList).toString)
      .addParameter("projection", projection.name)
      .execute
      .get

    val responseBody = response.getResponseBody
    if (responseBody.startsWith("{")) {
      val jsonResponse = new JSONObject()
      if (jsonResponse.has("id")) {
        val arr = new JSONArray(jsonResponse.getString("id"))
        (0 until arr.length()).map(i => arr.getString(i)).toList
      } else {
        null
      }
    } else {
      null
    }
  }

  def remove(projection: Projection, nodeIds: Set[Int], id: String)
  {}
}


object HttpIndexStorage
{
  def create(host: String) : HttpIndexStorage = {
    val indexStorage = new HttpIndexStorage(host)
    indexStorage.init
    indexStorage
  }
}