package io.stored.server.common

import org.json.JSONObject
import collection.immutable._
import collection.mutable.{LinkedHashMap, SynchronizedMap, SynchronizedSet, HashMap}


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
    val nodeMap = new HashMap[IndexStorage, collection.mutable.HashSet[Int] with SynchronizedSet[Int]] //with SynchronizedMap[IndexStorage, HashSet[Int] with SynchronizedSet[Int]]
    nodeIds.par.foreach { id =>
      val node = nodeHostMap.get(id).get
      if (!nodeMap.contains(node)) {
        nodeMap.synchronized {
          if (!nodeMap.contains(node)) {
            nodeMap.put(node, new collection.mutable.HashSet[Int] with SynchronizedSet[Int])
          }
        }
      }
      nodeMap.get(node).get.add(id)
    }
    nodeMap.toMap

    val resultMap = new HashMap[IndexStorage, Set[Int]]
    nodeMap.keySet.foreach( key => resultMap.put(key, nodeMap.get(key).get.toSet))
    resultMap.toMap
  }

  def getNodeIndexStorage(nodeId: Int) : IndexStorage = nodeHostMap.get(nodeId).get
}

object Projection {

  def determineNodeIds(dimensions: Int) : Set[Int] = {
    val ids = new collection.mutable.HashSet[Int]
    for (x <- 0 until math.pow(2, dimensions).toInt) ids.add(x)
    ids.toSet
  }

  def determineAllNodeIds(dimensions: Int) : Set[Int] = (0 until math.pow(2, dimensions).toInt).toSet

  def create(obj: JSONObject, nodesConfig: NodesConfig) : Projection = {

    val name = obj.getString("name")
    val dimensions = obj.getInt("dimensions")

    val fields = obj.getJSONObject("fields")
    val fieldMap = new LinkedHashMap[String, ProjectionField]
    val iter = fields.keys()
    while (iter.hasNext) {
      val key = iter.next().asInstanceOf[String]
      val internalKey = key.replaceAll("\\.", "__")
      fieldMap.put(internalKey, ProjectionField.create(internalKey, fields.get(key)))
    }

    val allNodeIds = determineAllNodeIds(dimensions)

    val localNodeIds = new collection.mutable.HashSet[Int] with SynchronizedSet[Int]
    val nodeMap = new collection.mutable.HashMap[Int, IndexStorage] with SynchronizedMap[Int, IndexStorage]
    allNodeIds.par.foreach{id =>
      val mod = id % nodesConfig.nodes.size
      if (mod == nodesConfig.localNodeIndex) localNodeIds.add(id)
      nodeMap.put(id, nodesConfig.hostStorage.get(mod).get)
    }

    new Projection(name, dimensions, fieldMap, allNodeIds, localNodeIds.toSet, nodeMap.toMap)
  }
}