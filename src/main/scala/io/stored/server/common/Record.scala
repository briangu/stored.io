package io.stored.server.common

import collection.mutable.HashMap
import org.json.JSONObject
import io.stored.common.CryptoUtil

object Record {
  def flatten(jsonData: JSONObject) : Map[String, AnyRef] = {
    flatten(jsonData, "", new HashMap[String, AnyRef])
  }

  def flatten(jsonData: JSONObject, path: String, map: HashMap[String, AnyRef]) : Map[String, AnyRef] = {
    val iter = jsonData.keys()

    while(iter.hasNext) {
      val key = iter.next().asInstanceOf[String]
      val refPath = "%s%s".format(path, key)
      val ref = jsonData.get(key)
      if (ref.isInstanceOf[JSONObject]) {
        flatten(ref.asInstanceOf[JSONObject], refPath + ".", map)
      } else {
        map.put(refPath, ref)
      }
    }

    map.toMap
  }

  def create(rawData: String) : Record = {
    val id = CryptoUtil.computeHash(rawData.getBytes("UTF-8"))
    val jsonData = new JSONObject(rawData)
    val colMap = flatten(jsonData)
    new Record(id, colMap, jsonData)
  }

  def create(jsonData: JSONObject) : Record = {
    val hash = CryptoUtil.computeHash(jsonData.toString.getBytes("UTF-8"))
    val keyMap = flatten(jsonData)
    new Record(hash, keyMap, jsonData)
  }
}

class Record(val id: String, val colMap: Map[String, AnyRef], val rawData: JSONObject) {

}
