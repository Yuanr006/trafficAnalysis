package cn.qphone.spark.bean

import scala.beans.BeanProperty

class RandomExtractCar {
  @BeanProperty
  var taskId:Long = null
  @BeanProperty
  var date:String = null
  @BeanProperty
  var car:String = null
  @BeanProperty
  var dateHour:String = null
}
