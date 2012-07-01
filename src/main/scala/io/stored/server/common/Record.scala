package io.stored.server.common

import collection.mutable.HashMap
import io.stored.common.CryptoUtil
import org.json.{JSONArray, JSONObject}


object Record {
  def flatten(jsonData: JSONObject) : Map[String, AnyRef] = {
    flatten(jsonData, "", new HashMap[String, AnyRef])
  }

  def flatten(jsonData: JSONObject, path: String, map: HashMap[String, AnyRef]) : Map[String, AnyRef] = {
    val iter = jsonData.keys()

    while(iter.hasNext) {
      val key = iter.next().asInstanceOf[String]
      val refPath = "%s%s".format(path, key.toUpperCase)
      val ref = jsonData.get(key)
      if (ref.isInstanceOf[JSONObject]) {
        flatten(ref.asInstanceOf[JSONObject], refPath + "__", map)
      } else if (ref.isInstanceOf[JSONArray]) {
        // TODO: make this work
        println("skipping jsonarray for key: " + refPath)
      } else {
        if (ref.isInstanceOf[String]
            || ref.isInstanceOf[Long]
            || ref.isInstanceOf[Int]
            || ref.isInstanceOf[Double]
            || ref.isInstanceOf[Boolean]) {
          map.put(refPath, ref)
        } else if (ref.equals(JSONObject.NULL)) {
          // nothing to do
        } else {
          println("skipping unknown %s for key: %s".format(ref.getClass.getName, refPath))
        }
      }
    }

    map.toMap
  }

  def canonicalJson(data: AnyRef) : String = {

    val sb = new StringBuilder

    if (data.isInstanceOf[JSONObject]) {

      val obj = data.asInstanceOf[JSONObject]
      val iter = obj.keys
      val keyMap = new HashMap[String, String]

      while(iter.hasNext) {
        val key = iter.next().asInstanceOf[String]
        val ref = obj.get(key)
        keyMap.put(key, canonicalJson(ref))
      }

      sb.append("{%s}".format(keyMap.keySet.toList.sorted.map(key => "\"%s\":%s".format(key, keyMap.get(key).get)).mkString(",")))
    } else if (data.isInstanceOf[JSONArray]) {
      val arr = data.asInstanceOf[JSONArray]
      sb.append("[%s]".format((0 until arr.length()).map(i => canonicalJson(arr.get(i)).mkString(","))))
    } else if (data.isInstanceOf[String]) {
      sb.append("\"%s\"".format(data.toString))
    } else {
      sb.append(data.toString)
    }

    sb.toString
  }

  def create(id: String, rawData: String) : Record = {
    val jsonData = new JSONObject(rawData)
    val colMap = flatten(jsonData)
    new Record(id, colMap, jsonData)
  }

  def create(jsonData: JSONObject) : Record = {
    val id = CryptoUtil.computeHash(canonicalJson(jsonData).getBytes("UTF-8"))
    val keyMap = flatten(jsonData)
    new Record(id, keyMap, jsonData)
  }

  def create(id: String, jsonData: JSONObject) : Record = {
    val keyMap = flatten(jsonData)
    new Record(id, keyMap, jsonData)
  }
}

class Record(val id: String, val colMap: Map[String, AnyRef], val rawData: JSONObject) {
  override def hashCode : Int = id.hashCode
  override def equals(other: Any) : Boolean = id.equals(other)
  def toJson : JSONObject = {
    val json = new JSONObject(rawData.toString) // TODO: faster way?
    json.put("__id", id)
    json
  }
}
