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

  def query(nodeIds: Set[Int], sql: String) = null

  def add(nodeIds: Set[Int], datum: Record)
  {}

  def addAll(nodeIds: Set[Int], data: List[Record])
  {}

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