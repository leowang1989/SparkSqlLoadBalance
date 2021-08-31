package org.apache.spark.sql.hive.thriftserver

import java.nio.charset.Charset
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.google.gson.Gson
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.Schedulable
import org.apache.spark.sql.SQLContext
import org.apache.spark.status.PoolData
import org.apache.spark.status.api.v1.ExecutorSummary

import scala.collection.mutable.ArrayBuffer


class LoadReporter(sqlContext: SQLContext, znode: PersistentEphemeralNode) extends Logging {
  // 负载上报线程池
  private var reportExec : ScheduledExecutorService  = _

  // 上报周期
  private val reportInterval : Int = 10

  val loadReportOp = new Runnable() {
    var lastLoadInfo : Option[InstanceLoadInfo] = None

    override def run(): Unit = {
      val sparkContext = sqlContext.sparkContext
      val loadInfo = new InstanceLoadInfo
      // 获取正在执行的SQL个数
      var runningSql = 0
      sqlContext.sharedState.statusStore.executionsList().foreach { e =>
        val isRunning = e.completionTime.isEmpty ||
          e.jobs.exists { case (_, status) => status == JobExecutionStatus.RUNNING }
        if (isRunning) {
          runningSql += 1
        }
      }

      // 获取当前正在运行的task个数和总核数
      var totalRunningTasks = 0
      var totalCores = 0
      sparkContext.statusStore.executorList(true).foreach((summary : ExecutorSummary) => {
        totalRunningTasks += summary.activeTasks
        totalCores += summary.totalCores
      })

      // 设置实例负载信息
      loadInfo.runningSqlNum = runningSql
      loadInfo.runningTaskNum = totalRunningTasks
      loadInfo.totalCoreNum = totalCores

      val conf = sparkContext.conf
      val dae = conf.getBoolean("spark.dynamicAllocation.enabled", false)
      val sse = conf.getBoolean("spark.shuffle.service.enabled", false)
      // 是否开启资源动态分配
      if (dae && sse) {
        val dame = conf.getInt("spark.dynamicAllocation.maxExecutors", 0)
        val ec = conf.getInt("spark.executor.cores", 0)
        loadInfo.maxCoreNum = dame * ec
      } else {
        loadInfo.maxCoreNum = totalCores
      }

      // 获取资源池负载信息
      val pools = Some(sparkContext).map(_.getAllPools).getOrElse(Seq.empty[Schedulable]).map { pool =>
        val uiPool = sparkContext.statusStore.asOption(sparkContext.statusStore.pool(pool.name)).getOrElse(
          new PoolData(pool.name, Set()))
        pool -> uiPool
      }.toMap

      val poolInfos = pools.map { element => {
          val (s, p) = element
          var poolInfo = new PoolLoadInfo
          poolInfo.poolName = s.name
          poolInfo.minShare = s.minShare
          poolInfo.poolWeight = s.weight
          poolInfo.mode = s.schedulingMode.toString
          poolInfo.activeStages = p.stageIds.size
          poolInfo.runningTaskNum = s.runningTasks
          poolInfo
        }
      }.toSeq

      // 根据资源池名排序
      loadInfo.poolLoadInfo = poolInfos.sortBy(_.poolName).to[ArrayBuffer]

      // 上报信息
      if (!lastLoadInfo.getOrElse(new InstanceLoadInfo).equals(loadInfo)) {
        val jsonStr = loadInfo.toJson
        znode.setData(jsonStr.getBytes(Charset.forName("UTF-8")))
        logInfo(s"report new load info: $jsonStr")
        lastLoadInfo = Some(loadInfo)
      } else {
        logInfo("load info has not changed.")
      }
    }
  }

  def start(): Unit = {
    logInfo("load reporter start...")
    // 启动负载上报线程池
    val threadFactory = new ThreadFactoryBuilder()
      .setNameFormat(s"LoadReportPool-%d")
      .build()
    reportExec = Executors.newSingleThreadScheduledExecutor(threadFactory);
    reportExec.scheduleWithFixedDelay(loadReportOp, 0, reportInterval, TimeUnit.SECONDS);
  }

  def stop(): Unit = {
    // 关闭负载上报线程池
    reportExec.shutdown();
    logInfo("load reporter stopped.")
  }
}

//object LoadReporter {
//  def main(args: Array[String]): Unit = {
//    var loadInfo = new InstanceLoadInfo
//    var pool = new PoolLoadInfo
//    pool.poolName = "default"
//    pool.mode = "FAIR"
//    loadInfo.poolLoadInfo = loadInfo.poolLoadInfo :+ pool
//
//    val jsonStr = loadInfo.toJson
//    println(jsonStr)
//  }
//}
