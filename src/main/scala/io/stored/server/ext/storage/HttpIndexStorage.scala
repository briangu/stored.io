package io.stored.server.ext.storage


import io.stored.server.common.{Record, IndexStorage}
import com.ning.http.client.AsyncHttpClient
import collection.mutable.ListBuffer
import org.json.{JSONArray, JSONObject}


class HttpIndexStorage(host: String) extends IndexStorage
{
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

  def query(nodeIds: Set[Int], sql: String) : List[Record] = {
    val asyncHttpClient = new AsyncHttpClient()
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

  def add(nodeIds: Set[Int], record: Record)
  {
    val asyncHttpClient = new AsyncHttpClient()
    val response = asyncHttpClient
      .preparePost("%s/records".format(host))
      .addParameter("record", record.rawData.toString)
      .addParameter("nodeIds", toJsonArray(nodeIds).toString)
      .execute
      .get
    val jsonResponse = new JSONObject(response.getResponseBody)
    val id = jsonResponse.getString("id")
  }

  def addAll(nodeIds: Set[Int], records: List[Record])
  {
    records.foreach(add(null, _))
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