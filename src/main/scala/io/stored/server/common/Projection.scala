package io.stored.server.common

import org.json.JSONObject
import io.stored.server.ext.storage.HttpIndexStorage
import collection.mutable.{SynchronizedSet, HashMap, SynchronizedMap, LinkedHashMap}


class Projection(
  val name: String,
  val dimensions: Int,
  fields: collection.mutable.LinkedHashMap[String, ProjectionField],
  val allNodeIds: Set[Int],
  val localNodeIds: Set[Int],
  val nodeHostMap: Map[Int, IndexStorage]) {

  def getFields = fields.keySet
  def getFieldValue(key: String) = fields.get(key).get

  def getNodeHosts(nodeIds: Set[Int]) : Map[IndexStorage, Set[Int]] = {
    val nodeMap = new collection.mutable.HashMap[IndexStorage, Set[Int]] with SynchronizedMap[IndexStorage, Set[Int]]

    // TODO: fix me

    nodeMap.toMap
  }

  def getNodeIndexStorage(nodeId: Int) : IndexStorage = nodeHostMap.get(nodeId).get
}

object Projection {

  def determineNodeIds(dimensions: Int) : Set[Int] = {
    val ids = new collection.mutable.HashSet[Int]
    for (x <- 0 until math.pow(2, dimensions).toInt) ids.add(x)
    ids.toSet
  }

  def determineAllNodeIds(dimensions: Int) : Set[Int] = {
    val ids = new collection.mutable.HashSet[Int]
    for (x <- 0 until math.pow(2, dimensions).toInt) ids.add(x)
    ids.toSet
  }

  def create(obj: JSONObject, nodesConfig: NodesConfig) : Projection = {

    val name = obj.getString("name")
    val dimensions = obj.getInt("dimensions")

    val fields = obj.getJSONObject("fields")
    val fieldMap = new LinkedHashMap[String, ProjectionField]
    val iter = fields.keys()
    while (iter.hasNext) {
      val key = iter.next().asInstanceOf[String];
      fieldMap.put(key, ProjectionField.create(key, fields.get(key)))
    }

    val allNodeIds = determineAllNodeIds(dimensions)

    val localNodeIds = new collection.mutable.HashSet[Int] with SynchronizedSet[Int]
    val nodeMap = new collection.mutable.HashMap[Int, IndexStorage] with SynchronizedMap[Int, IndexStorage]
    allNodeIds.par.foreach{id =>
      val mod = id % dimensions
      if (mod == nodesConfig.localNodeIndex) localNodeIds.add(id)
      nodeMap.put(id, nodesConfig.hostStorage.get(mod).get)
    }

    new Projection(name, dimensions, fieldMap, allNodeIds, localNodeIds.toSet, nodeMap.toMap)
  }
}