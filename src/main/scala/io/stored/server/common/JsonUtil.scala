package io.stored.server.common

import io.viper.core.server.router.JsonResponse
import org.json.{JSONObject, JSONArray}
import collection.mutable.ListBuffer

object JsonUtil {

  def toJsonArray[A,B](list: List[A]) : JSONArray = {
    toJsonArray(list, {x:A => x})
  }

  def toJsonArray[A,B](list: List[A], f: A => B) : JSONArray = {
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
