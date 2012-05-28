package io.stored.server.common

trait IndexStorage {
  def init()
  def shutdown()

  def purge()

  def query(projection: Projection, nodeIds: Set[Int], sql: String) : List[Record]

  def add(projection: Projection, nodeIds: Set[Int], record: Record) : String
  def addAll(projection: Projection, nodeIdMap: Map[Int, Set[String]], recordMap: Map[String, Record]) : List[String]

  def remove(nodeIds: Set[Int], id: String)
}
