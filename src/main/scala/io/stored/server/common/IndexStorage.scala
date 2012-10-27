package io.stored.server.common


trait IndexStorage {
  def init()
  def shutdown()

  def purge()

  def jsonQuery(projection: Projection, nodeIds: Set[Int], sql: String) : String
  def query(projection: Projection, nodeIds: Set[Int], sql: String) : List[Record]

  def add(projection: Projection, nodeIdMap: Map[Int, Set[String]], record: Record) : String = {
    val res = addAll(projection, nodeIdMap, Map(record.id -> record))
    if (res == null) { null } else {res(0)}
  }
  def addAll(projection: Projection, nodeIdMap: Map[Int, Set[String]], recordMap: Map[String, Record]) : List[String]

  def remove(projection: Projection, nodeIds: Set[Int], id: String) {
    remove(projection, nodeIds, List(id))
  }

  def remove(projection: Projection, nodeIds: Set[Int], ids: List[String])
}
