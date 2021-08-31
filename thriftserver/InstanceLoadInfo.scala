package org.apache.spark.sql.hive.thriftserver


import java.lang.reflect.Type

import com.google.gson.{Gson, GsonBuilder, JsonElement, JsonSerializationContext, JsonSerializer}

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer


class InstanceLoadInfo {
  // 正在执行的SQL个数
  @BeanProperty var runningSqlNum : Int = 0

  // 正在运行的Task个数
  @BeanProperty var runningTaskNum : Int = 0

  // 当前总核数
  @BeanProperty var totalCoreNum : Int = 0

  // 最大核数
  @BeanProperty var maxCoreNum : Int = 0

  // 资源池负载
  @BeanProperty var poolLoadInfo : ArrayBuffer[PoolLoadInfo] = new ArrayBuffer[PoolLoadInfo]

  class PoolLoadsSerializer extends JsonSerializer[ArrayBuffer[PoolLoadInfo]] {
    override def serialize(src: ArrayBuffer[PoolLoadInfo], typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      context.serialize(src.toArray)
    }
  }

  def toJson : String = {
    val gson = new GsonBuilder().registerTypeAdapter(classOf[ArrayBuffer[PoolLoadInfo]], new PoolLoadsSerializer).setPrettyPrinting().create()
    gson.toJson(this, classOf[InstanceLoadInfo])
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      return false
    } else if (!obj.isInstanceOf[InstanceLoadInfo]) {
      return false
    } else {
      val other = obj.asInstanceOf[InstanceLoadInfo]

      if (!(this.runningSqlNum == other.runningSqlNum &&
        this.runningTaskNum == other.runningTaskNum &&
        this.totalCoreNum == other.totalCoreNum &&
        this.maxCoreNum == other.maxCoreNum &&
        this.poolLoadInfo.size == other.poolLoadInfo.size)) {
        return false
      }

      for (index <- 0 until this.poolLoadInfo.size) {
        val info1 = this.poolLoadInfo(index)
        val info2 = other.poolLoadInfo(index)

        if (!info1.equals(info2)) {
          return false
        }
      }

      return true
    }
  }
}
