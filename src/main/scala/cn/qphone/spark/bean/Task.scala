package cn.qphone.spark.bean

import scala.beans.BeanProperty

class Task {
  @BeanProperty
  var taskId:Long = null
  @BeanProperty
  var taskName:String = null
  @BeanProperty
  var createTime:String = null
  @BeanProperty
  var startTime:String = null
  @BeanProperty
  var finishTime:String = null
  @BeanProperty
  var taskType:String = null
  @BeanProperty
  var taskStatus:String = null
  @BeanProperty
  var taskParams:String = null
}
