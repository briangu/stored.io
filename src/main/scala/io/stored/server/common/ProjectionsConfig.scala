package io.stored.server.common


import io.stored.common.FileUtil
import scala.Predef._
import collection.mutable.ListBuffer
import org.json.{JSONArray, JSONObject}


class ProjectionsConfig(projections: Map[String, Projection], val defaultProjection: Projection)
{
  def getDefaultProjection: Projection = defaultProjection
  def hasProjection(name: String) : Boolean = projections.contains(name)
  def getProjection(name: String) : Projection = if (projections.contains(name)) projections.get(name).get else null
}

object ProjectionsConfig
{
  def create(localNode: IndexStorage, projectionsConfigFile: String) : ProjectionsConfig = {
    val projectionConfig = FileUtil.readJson(projectionsConfigFile)
    create(localNode, projectionConfig)
  }

  def create(localNode: IndexStorage, projectionsConfig: JSONObject) : ProjectionsConfig = {
    val localhost = "localhost"
    val nodes = new JSONArray()
    nodes.put(localhost)

    val nodesConfig = NodesConfig.create(localhost, localNode, nodes)
    create(localhost, localNode, nodesConfig, projectionsConfig)
  }

  def create(localhost: String, localNode: IndexStorage, nodesConfigFile: String, projectionsConfigFile: String) : ProjectionsConfig = {
    val nodesConfig = NodesConfig.create(localhost, localNode, FileUtil.readJsonArray(nodesConfigFile))
    create(localhost, localNode, nodesConfig, projectionsConfigFile)
  }

  def create(localhost: String, localNode: IndexStorage, nodesConfigFile: String, projectionsConfig: JSONObject) : ProjectionsConfig = {
    val nodesConfig = NodesConfig.create(localhost, localNode, FileUtil.readJsonArray(nodesConfigFile))
    create(localhost, localNode, nodesConfig, projectionsConfig)
  }

  private def create(localhost: String, localNode: IndexStorage, nodesConfig: NodesConfig, projectionsConfigFile: String) : ProjectionsConfig = {
    val projectionConfig = FileUtil.readJson(projectionsConfigFile)
    create(localhost, localNode, nodesConfig, projectionConfig)
  }

  private def create(localhost: String, localNode: IndexStorage, nodesConfig: NodesConfig, projectionsConfig: JSONObject) : ProjectionsConfig = {

    val projections = new collection.mutable.HashMap[String, Projection]

    val projArr = projectionsConfig.getJSONArray("projections")
    (0 until projArr.length()).foreach { i =>
      val projection = Projection.create(projArr.getJSONObject(i), nodesConfig)
      projections.put(projection.name, projection)
    }

    val defaultProjection = projections.get(projectionsConfig.getString("default")).get

    new ProjectionsConfig(projections.toMap, defaultProjection)
  }
}
