package io.stored.server.common

import org.json.JSONObject


trait IndexStorage {
  def init(configRoot: String)
  def shutdown()

  def purge()

  def query(filter: JSONObject) : List[Record]
  def query(sql: String) : List[Record]

  def add(datum: Record)
  def addAll(data: List[Record])

  def remove(id: String)
}
