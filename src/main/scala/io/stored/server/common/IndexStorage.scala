package io.stored.server.common

import org.json.JSONObject


trait IndexStorage {
  def init()
  def shutdown()

  def purge()

  def query(nodeIds: Set[Int], sql: String) : List[Record]

  def add(nodeIds: Set[Int], datum: Record)
  def addAll(nodeIds: Set[Int], data: List[Record])

  def remove(nodeIds: Set[Int], id: String)
}
