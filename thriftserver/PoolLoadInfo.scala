package org.apache.spark.sql.hive.thriftserver

import com.google.gson.Gson

import scala.beans.BeanProperty


class PoolLoadInfo {
  // 资源池名
  @BeanProperty var poolName : String = _

  // 最小核数
  @BeanProperty var minShare : Int = 0

  // 权重
  @BeanProperty var	poolWeight : Int = 0

  // 调度模式
  @BeanProperty var mode : String = _

  // 正在运行的SQL个数
  @BeanProperty var activeStages : Int = 0

  // 正在运行的task个数
  @BeanProperty var runningTaskNum : Int = 0

  def toJson : String = {
    val gson = new Gson
    gson.toJson(this)
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      return false
    } else if (!obj.isInstanceOf[PoolLoadInfo]) {
      return false
    } else {
      val other = obj.asInstanceOf[PoolLoadInfo]
      return this.poolName.equals(other.poolName) && this.minShare == other.minShare &&
        this.poolWeight == other.poolWeight && this.mode.equals(other.mode) &&
        this.activeStages == other.activeStages && this.runningTaskNum == other.runningTaskNum
    }
  }
}
