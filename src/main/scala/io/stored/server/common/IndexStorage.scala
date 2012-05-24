package io.stored.server.common

trait IndexStorage {
  def init()
  def shutdown()

  def purge()

  def query(projection: Projection, nodeIds: Set[Int], sql: String) : List[Record]

  def add(projection: Projection, nodeIds: Set[Int], datum: Record) : String
  def addAll(projection: Projection, nodeIds: Set[Int], data: List[Record]) : List[String]

  def remove(nodeIds: Set[Int], id: String)
}
