package io.stored.server.common

import util.parsing.json.JSONObject
import java.security.MessageDigest

class ProjectionField(val name: String, val bitWeight: Int) {

}

object ProjectionField {

  def md5Hash(v: Double) : BigInt = md5Hash(BigInt.apply(v.toLong).toByteArray)
  def md5Hash(v: Long) : BigInt = md5Hash(BigInt.apply(v).toByteArray)
  def md5Hash(v: java.lang.Integer) : BigInt = md5Hash(BigInt.apply(v).toByteArray)
  def md5Hash(str: String) : BigInt = md5Hash(str.getBytes("UTF-8"))

  def md5Hash(record: Array[Byte]) : BigInt = {
    val m = MessageDigest.getInstance("MD5");
    m.reset();
    m.update(record);
    val digest = m.digest();
    BigInt.apply(digest)
  }

  def create(name: String, obj: Object) : ProjectionField = {
    if (obj.isInstanceOf[Int]) {
      new ProjectionField(name, obj.asInstanceOf[Int])
    } else if (obj.isInstanceOf[JSONObject]) {
      throw new RuntimeException("obj type not supported")
    } else {
      throw new RuntimeException("obj type not supported")
    }
  }
}