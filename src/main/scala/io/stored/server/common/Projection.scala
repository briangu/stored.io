package io.stored.server.common

import org.json.JSONObject
import collection.immutable._
import collection.mutable.{LinkedHashMap, SynchronizedMap, SynchronizedSet}


class Projection(
  val name: String,
  val dimensions: Int,
  fields: collection.mutable.LinkedHashMap[String, ProjectionField],
  val allNodeIds: Set[Int],
  val localNodeIds: Set[Int],
  val nodeHostMap: Map[Int, IndexStorage]) {

  def getFields = fields.keySet
  def getFieldValue(key: String) = fields.get(key).get

  def getNodeStores(nodeIds: Set[Int]) : Map[IndexStorage, Set[Int]] = {
    nodeIds.flatMap(id => Map(nodeHostMap.get(id).get -> Set(id))).toMap
  }

  def getNodeIndexStorage(nodeId: Int) : IndexStorage = nodeHostMap.get(nodeId).get

  def getRecordCoords(data: Map[String, AnyRef]) : Map[String, List[BigInt]] = {
    data.keySet.flatMap{ key =>
      val dataVal = data.get(key).get

      if (dataVal.isInstanceOf[Long]) {
        Map(key -> List(ProjectionField.md5Hash(dataVal.asInstanceOf[Long])))
      } else if (dataVal.isInstanceOf[java.lang.Integer]) {
        Map(key -> List(ProjectionField.md5Hash(dataVal.asInstanceOf[java.lang.Integer])))
      } else if (dataVal.isInstanceOf[String]) {
        Map(key -> List(ProjectionField.md5Hash(dataVal.asInstanceOf[String])))
      } else if (dataVal.isInstanceOf[Double]) {
        Map(key -> List(ProjectionField.md5Hash(dataVal.asInstanceOf[Double])))
      } else {
        // println("not hashing col: " + key)
        // skip fields we can't hash
        Nil
      }
    }.toMap
  }

  def getNodeIds(record: Record) : Set[Int] = {
    getNodeIds(getRecordCoords(record.colMap))
  }

  def getNodeIds(hashCoords: Map[String, List[BigInt]]) : Set[Int] = {
    if (hashCoords == null || hashCoords.size == 0) return allNodeIds

    val intersection = hashCoords.keySet.intersect(getFields)
    if (intersection.size == 0) return allNodeIds

    // fill each project field with all appropriate values
    val fieldMap = getFields.flatMap{ key =>
      if (hashCoords.contains(key)) {
        Map(key -> hashCoords.get(key).get)
      } else {
        Map(key -> genFieldValues(getFieldValue(key).bitWeight))
      }
    }.toMap

    val fieldList = getFields.toList

    // generate all possible nodeIds using the fieldMap
    val nodeIds = new collection.mutable.HashSet[Int]
    genNodeIds(fieldList, fieldMap, nodeIds, 0)
    nodeIds.toSet
  }

  def genFieldValues(bitwidth: Int) : List[BigInt] = {
    val ids = new collection.mutable.HashSet[BigInt]
    for (x <- 0 until math.pow(2, bitwidth).toInt) ids.add(BigInt.apply(x))
    ids.toList
  }

  def genNodeIds(fields: List[String], fieldMap: Map[String, List[BigInt]], nodeIds: collection.mutable.HashSet[Int], nodeIdPrefix: Int) {
    if (fields.size == 0) {
      nodeIds.add(nodeIdPrefix)
      return
    }

    val bitWidth = getFieldValue(fields(0)).bitWeight
    val shifted = BigInt.apply(nodeIdPrefix << bitWidth)
    val mask = (math.pow(2, bitWidth).toInt - 1)

    fieldMap.get(fields(0)).get.foreach{ bitVal =>
      val nodeId = shifted | (bitVal & mask)
      genNodeIds(fields.slice(1, fields.size), fieldMap, nodeIds, nodeId.toInt)
    }
  }
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
      val internalKey = key.replaceAll("\\.", "__").toUpperCase
      fieldMap.put(internalKey, ProjectionField.create(internalKey, fields.get(key)))
    }

    val allNodeIds = determineAllNodeIds(dimensions)

    // TODO: we should be using a hamming distance function not mod (or use a strategy plugin)
    val localNodeIds = allNodeIds.filter(id => (id % nodesConfig.nodes.size) == nodesConfig.localNodeIndex)
    val nodeMap = allNodeIds.flatMap(id => Map(id -> nodesConfig.hostStorage.get(id % nodesConfig.nodes.size).get))

    new Projection(name, dimensions, fieldMap, allNodeIds, localNodeIds.toSet, nodeMap.toMap)
  }
}