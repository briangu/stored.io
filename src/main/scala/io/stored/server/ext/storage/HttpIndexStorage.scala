package io.stored.server.ext.storage


import io.stored.server.common.{Record, IndexStorage}
import com.ning.http.client.AsyncHttpClient
import org.json.JSONObject
import collection.mutable.ListBuffer


class HttpIndexStorage(host: String) extends IndexStorage
{
  def init()
  {}

  def shutdown()
  {}

  def purge()
  {}

  def query(nodeIds: Set[Int], sql: String) : List[Record] = {
    val asyncHttpClient = new AsyncHttpClient()
    val f = asyncHttpClient.preparePost("%s/records/queries".format(host)).addParameter("sql",sql).execute
    val response = f.get
    val jsonResponse = new JSONObject(response.getResponseBody)
    val elements = jsonResponse.getJSONArray("elements")

    val results = new ListBuffer[Record]
    (0 until elements.length()).foreach(i => results.append(Record.create(elements.getJSONObject(i))))
    results.toList
  }

  def add(nodeIds: Set[Int], record: Record)
  {
    val asyncHttpClient = new AsyncHttpClient()
    val f = asyncHttpClient.preparePost("%s/records".format(host)).addParameter("record",record.rawData.toString).execute
    val response = f.get
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