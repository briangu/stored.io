package io.stored.server.common


import org.json.JSONArray
import collection.mutable.{HashMap, ListBuffer}
import io.stored.server.ext.storage.HttpIndexStorage


class NodesConfig(val localNode: IndexStorage, val localNodeIndex: Int, val nodes: List[IndexStorage], val hostStorage: Map[Int, IndexStorage]) {
}

object NodesConfig {
  def loadNodeMachines(nodeConfig: JSONArray) : List[String] = {
    val result = new ListBuffer[String]
    (0 until nodeConfig.length()).foreach( i=> result.append(nodeConfig.getString(i)))
    result.toList
  }

  def create(localhost: String, localNode: IndexStorage, nodesConfig: JSONArray) : NodesConfig = {

    val nodeHosts = loadNodeMachines(nodesConfig)

    val nodes = new ListBuffer[IndexStorage]
    val hostStorage = new HashMap[Int, IndexStorage]
    var localNodeIndex : Int = 0
    (0 until nodeHosts.size).foreach{ i =>
      val host = nodeHosts(i)
      if (host.equals(localhost)) {
        localNodeIndex = i
        nodes.append(localNode)
      } else {
        val storage = HttpIndexStorage.create(host)
        hostStorage.put(i, storage)
        nodes.append(storage)
      }
    }

    new NodesConfig(localNode, localNodeIndex, nodes.toList, hostStorage.toMap)
  }
}
