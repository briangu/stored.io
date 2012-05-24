package io.stored.server.common


import io.stored.common.FileUtil
import scala.Predef._
import collection.mutable.ListBuffer
import org.json.{JSONArray, JSONObject}


class ProjectionsConfig(projections: Map[String, Projection], val defaultProjection: Projection)
{
  def getDefaultProjection: Projection = defaultProjection
  def hasProjection(name: String) : Boolean = projections.contains(name)
  def getProjection(name: String) : Projection = projections.get(name).get
}

object ProjectionsConfig
{
  def create(localhost: String, localNode: IndexStorage, nodesConfigFile: String, projectionsConfigFile: String) : ProjectionsConfig = {
    val nodesConfig = NodesConfig.create(localhost, localNode, FileUtil.readJsonArray(nodesConfigFile))
    val projectionConfig = FileUtil.readJson(projectionsConfigFile)

    val projections = new collection.mutable.HashMap[String, Projection]

    val projArr = projectionConfig.getJSONArray("projections")
    (0 until projArr.length()).foreach { i =>
      val projection = Projection.create(projArr.getJSONObject(i), nodesConfig)
      projections.put(projection.name, projection)
    }

    val defaultProjection = projections.get(projectionConfig.getString("default")).get

    new ProjectionsConfig(projections.toMap, defaultProjection)
  }
}
