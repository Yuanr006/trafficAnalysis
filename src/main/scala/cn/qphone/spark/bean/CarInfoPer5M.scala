package cn.qphone.spark.bean

import scala.beans.BeanProperty

class CarInfoPer5M {
 def this(id:Long,moniId:String,car:String) {
   this
   taskId= id
   monitorId=moniId
   cars = car
 }

  private[this] var _taskId: Long = 0

  def taskId: Long = _taskId

  def taskId_=(value: Long): Unit = {
    _taskId = value
  }

  private[this] var _monitorId: String = ""

  def monitorId: String = _monitorId

  def monitorId_=(value: String): Unit = {
    _monitorId = value
  }

  private[this] var _rangeTime: String = ""

  def rangeTime: String = _rangeTime

  def rangeTime_=(value: String): Unit = {
    _rangeTime = value
  }

  private[this] var _cars: String = ""

  def cars: String = _cars

  def cars_=(value: String): Unit = {
    _cars = value
  }

  override def toString = s"CarInfoPer5M($taskId, $monitorId, $rangeTime, $cars)"
}
