package io.stored.server.ext.storage

import io.stored.server.common.{Record, Projection, IndexStorage}
import collection.mutable.{SynchronizedMap, HashMap}

/***
 * This is an experimental IndexStorage that creates a new DB for each projection.
 * PROS: This has the upside of creating smaller Dbs
 * CONS: Until the data is pulled out as a seperate level of indirection, a downside is that the same data may be
 *       Stored multiple times over multiple projection dbs
 */
object H2ProjectionStorageRouter {
  def create(configRoot: String) : IndexStorage = {
    val storage = new H2ProjectionStorageRouter(configRoot)
    storage.init()
    storage
  }
}

class H2ProjectionStorageRouter(configRoot: String) extends IndexStorage {
  private val _projections = new HashMap[String, IndexStorage] with SynchronizedMap[String, IndexStorage]

  def init() {}

  def shutdown() {
    _projections.values.foreach(_.shutdown)
  }

  def purge() {
    _projections.values.foreach(_.purge)
  }

  def query(projection: Projection, nodeIds: Set[Int], sql: String): List[Record] = {
    getProjStore(projection.name).query(projection, nodeIds, sql)
  }

  def addAll(projection: Projection, nodeIdMap: Map[Int, Set[String]], recordMap: Map[String, Record]): List[String] = {
    getProjStore(projection.name).addAll(projection, nodeIdMap, recordMap)
  }

  def remove(projection: Projection, nodeIds: Set[Int], ids: List[String]) {
    getProjStore(projection.name).remove(projection, nodeIds, ids)
  }

  def getProjStore(projectionName: String) : IndexStorage = {
    if (!_projections.contains(projectionName)) {
      _projections.synchronized {
        if (!_projections.contains(projectionName)) {
          _projections.put(projectionName, H2IndexStorage.create("%s_%s".format(configRoot, projectionName)))
        }
      }
    }
    _projections.get(projectionName).get
  }
}
