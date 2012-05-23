package io.stored.server.ext.storage


import io.stored.server.common.{Record, IndexStorage}
import org.json.JSONObject


class HttpIndexStorage(host: String) extends IndexStorage
{
  def init()
  {}

  def shutdown()
  {}

  def purge()
  {}

  def query(filter: JSONObject) = null

  def query(sql: String) = null

  def add(datum: Record)
  {}

  def addAll(data: List[Record])
  {}

  def remove(id: String)
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