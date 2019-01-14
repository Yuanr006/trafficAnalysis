package cn.qphone.spark.bean

import scala.beans.BeanProperty

class CarInfoPer5M {
  @BeanProperty
  var taskId:Long = null
  @BeanProperty
  var monitorId:String = null
  @BeanProperty
  var rangeTime:String = null
  @BeanProperty
  val cars:String = null
}
