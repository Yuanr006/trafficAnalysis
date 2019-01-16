package cn.qphone.spark.dao.impl

import java.util

import cn.qphone.spark.bean.{MonitorState, TopNMonitor2CarCount, TopNMonitorDetailInfo}
import cn.qphone.spark.dao.IMonitorDAO
import cn.qphone.spark.jdbc.JDBCHelper

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/15 19:53
  * @Description: ${Description}
  */
class MonitorDAOImpl extends IMonitorDAO{
  /**
    * 卡口流量topN批量插入到数据库
    *
    * @param topNMonitor2CarCounts
    */
  override def insertBatchTopN(topNMonitor2CarCounts: List[TopNMonitor2CarCount]): Unit =  {
    val sql = "INSERT INTO topn_monitor_car_count VALUES(?,?,?)";
    val params = new ListBuffer[Array[String]]
    for ( topNMonitor2CarCount <- topNMonitor2CarCounts) {
      val arr = ArrayBuffer[String]()
       arr.append(
        topNMonitor2CarCount.taskId.toString,
        topNMonitor2CarCount.monitorId,
        topNMonitor2CarCount.carCount)
      params.append(arr.toArray)
    }
    JDBCHelper.executeBatch(sql , params.toList)
  }

  /**
    * 卡口下车辆具体信息插入到数据库
    *
    * @param monitorDetailInfos
    */
  override def insertBatchMonitorDetails(monitorDetailInfos: List[TopNMonitorDetailInfo]): Unit = ???

  /**
    * 卡口状态信息插入到数据库
    *
    * @param monitorState
    */
override def insertMonitorState(monitorState: MonitorState): Unit = {
  val sql = "INSERT INTO monitor_state VALUES(?,?,?,?,?,?)"
  val param =  Array[String](
    monitorState.taskId.toString,
    monitorState.normalMonitorCount.toString,
    monitorState.normalCameraCount.toString,
    monitorState.abnormalMonitorCount.toString,
    monitorState.abnormalCameraCount.toString,
    monitorState.abnormalMonitorCameraInfos.toString)
  val params = ListBuffer[Array[String]]()
  params.append(param)
  JDBCHelper.executeBatch(sql, params.toList)
}
override def insertBatchTop10Details(topNMonitorDetailInfos: List[TopNMonitorDetailInfo]): Unit = ???
}
