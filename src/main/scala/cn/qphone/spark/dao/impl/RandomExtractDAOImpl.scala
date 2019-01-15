package cn.qphone.spark.dao

import cn.qphone.spark.bean.{MonitorState, TopNMonitor2CarCount, TopNMonitorDetailInfo}

trait IMonitorDAO {
  /**
    * 卡口流量topN批量插入到数据库
    * @param topNMonitor2CarCounts
    */
  def insertBatchTopN(topNMonitor2CarCounts:List[TopNMonitor2CarCount] )

  /**
    * 卡口下车辆具体信息插入到数据库
    * @param monitorDetailInfos
    */
  def insertBatchMonitorDetails(monitorDetailInfos:List[TopNMonitorDetailInfo])


  /**
    * 卡口状态信息插入到数据库
    * @param monitorState
    */
  def insertMonitorState(monitorState:MonitorState );

  def insertBatchTop10Details(topNMonitorDetailInfos:List[TopNMonitorDetailInfo] );
}
