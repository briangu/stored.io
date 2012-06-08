package io.stored.server.common

import io.viper.core.server.router.JsonResponse
import collection.mutable.ListBuffer
import org.json.{JSONObject, JSONArray}

object JsonUtil {

  def toJsonArray[A,B](list: List[A]) : JSONArray = {
    val arr = new JSONArray
    list.foreach{x: A => arr.put(x)}
    arr
  }

  def toJsonArray[A,JSONObject](list: List[A], f: A => JSONObject) : JSONArray = {
    val arr = new JSONArray
    list.foreach{x: A => arr.put(f(x))}
    arr
  }

  def jsonResponse(args: AnyRef*) : JsonResponse = {
    if (args.length % 2 != 0) throw new RuntimeException("expecting key value pairs")
    val obj = new JSONObject
    (0 until args.length by 2).foreach(i => obj.put(args(i).toString(), args(i+1)))
    new JsonResponse(obj)
  }

  def toJsonArray(set: Set[Int]) : JSONArray = {
    val jsonArray = new JSONArray
    set.foreach(jsonArray.put)
    jsonArray
  }

  def intSetFromJsonArray(rawJsonArray : String) : Set[Int] = {
    val ja = new JSONArray(rawJsonArray)
    (0 until ja.length()).map(i => ja.getInt(i)).toSet
  }
}
