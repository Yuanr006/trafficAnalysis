package cn.qphone.spark.bean

import scala.beans.BeanProperty

class TopNMonitor2CarCount {
  @BeanProperty
  var taskId:Long = null
  @BeanProperty
  var monitorId:String = null
  @BeanProperty
  var carCount:String = null
}
