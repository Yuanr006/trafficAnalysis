package cn.qphone.spark.bean

/**
  * 保存车辆轨迹信息 domain
  * @author root
  */

class CarTrack {
  def this(id:Long,dt:String,ca:String,tk:String) {
    this
    taskId= id
    date=dt
    car = ca
    track=tk
  }

  private[this] var _taskId: Long = 0

  def taskId: Long = _taskId

  def taskId_=(value: Long): Unit = {
    _taskId = value
  }

  private[this] var _date = ""

  def date: String = _date

  def date_=(value: String): Unit = {
    _date = value
  }

  private[this] var _car = ""

  def car: String = _car

  def car_=(value: String): Unit = {
    _car = value
  }

  private[this] var _track = ""

  def track: String = _track

  def track_=(value: String): Unit = {
    _track = value
  }

  override def toString = s"CarTrack($taskId, $date, $car, $track)"
}
