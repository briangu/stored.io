package io.stored.server.common

import util.parsing.json.JSONObject
import com.sun.javaws.exceptions.InvalidArgumentException

class SchemaField(name: String, bitWeight: Int) {

}

object SchemaField {
  def create(name: String, obj: Object) : SchemaField = {
    if (obj.isInstanceOf[Int]) {
      new SchemaField(name, obj.asInstanceOf[Int])
    } else if (obj.isInstanceOf[JSONObject]) {
      throw new InvalidArgumentException(Array("obj type not supported"))
    } else {
      throw new InvalidArgumentException(Array("obj type not supported"))
    }
  }
}