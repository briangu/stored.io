package io.stored.server.common

import org.json.JSONObject
import collection.mutable.HashMap


class Schema(val dimensions: Int, val fields: Map[String, SchemaField]) {

}

object Schema {
  def create(obj: JSONObject) : Schema = {

    val dimensions = obj.getInt("dimensions")
    val fields = obj.getJSONObject("fields")

    val fieldMap = new HashMap[String, SchemaField]

    val iter = fields.keys()
    while (iter.hasNext) {
      val key = iter.next().asInstanceOf[String];
      fieldMap.put(key, SchemaField.create(key, fields.get(key)))
    }

    new Schema(dimensions, fieldMap.toMap)
  }
}