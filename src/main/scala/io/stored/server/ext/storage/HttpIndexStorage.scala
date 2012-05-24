package io.stored.server.ext.storage


import com.ning.http.client.AsyncHttpClient
import collection.mutable.ListBuffer
import org.json.{JSONArray, JSONObject}
import io.stored.server.common.{Projection, Record, IndexStorage}


class HttpIndexStorage(host: String) extends IndexStorage
{
  val asyncHttpClient = new AsyncHttpClient()

  def init()
  {}

  def shutdown()
  {}

  def purge()
  {}

  def toJsonArray(set: Set[Int]) : JSONArray = {
    val jsonArray = new JSONArray
    set.foreach(jsonArray.put)
    jsonArray
  }

  def query(projection: Projection, nodeIds: Set[Int], sql: String) : List[Record] = {
    val response = asyncHttpClient.
      preparePost("%s/records/queries".format(host))
      .addParameter("sql",sql)
      .addParameter("nodeIds", toJsonArray(nodeIds).toString)
      .execute
      .get
    val jsonResponse = new JSONObject(response.getResponseBody)
    val elements = jsonResponse.getJSONArray("elements")

    val results = new ListBuffer[Record]
    (0 until elements.length()).foreach(i => results.append(Record.create(elements.getJSONObject(i))))
    results.toList
  }

  def add(projection: Projection, nodeIds: Set[Int], record: Record) : String = {
    val response = asyncHttpClient
      .preparePost("%s/records".format(host))
      .addParameter("record", record.rawData.toString)
      .addParameter("projection", projection.name)
      .addParameter("nodeIds", toJsonArray(nodeIds).toString)
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

  def addAll(projection: Projection, nodeIds: Set[Int], records: List[Record]) : List[String] = {
    records.foreach(add(projection, null, _))
    null
  }

  def remove(nodeIds: Set[Int], id: String)
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