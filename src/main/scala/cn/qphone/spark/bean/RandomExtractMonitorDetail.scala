package cn.qphone.spark.bean

import scala.beans.BeanProperty

class RandomExtractMonitorDetail {
  @BeanProperty
  var taskId:Long = null
  @BeanProperty
  var date:String = null
  @BeanProperty
  var car:String = null
  @BeanProperty
  var monitorId:String = null
  @BeanProperty
  var cameraId:String = null
  @BeanProperty
  var actionTime:String = null
  @BeanProperty
  var speed:String = null
  @BeanProperty
  var roadId:String = null
}
