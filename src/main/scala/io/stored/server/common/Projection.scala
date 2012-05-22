package io.stored.server.common

import org.json.JSONObject
import collection.mutable.LinkedHashMap


class Projection(val dimensions: Int, fields: collection.mutable.LinkedHashMap[String, ProjectionField]) {

  def getFields = fields.keySet
  def getFieldValue(key: String) = fields.get(key).get

}

object Projection {
  def create(obj: JSONObject) : Projection = {

    val dimensions = obj.getInt("dimensions")
    val fields = obj.getJSONObject("fields")

    val fieldMap = new LinkedHashMap[String, ProjectionField]

    val iter = fields.keys()
    while (iter.hasNext) {
      val key = iter.next().asInstanceOf[String];
      fieldMap.put(key, ProjectionField.create(key, fields.get(key)))
    }

    new Projection(dimensions, fieldMap)
  }
}