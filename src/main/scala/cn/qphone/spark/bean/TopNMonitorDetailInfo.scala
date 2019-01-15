package cn.qphone.spark.bean

class TopNMonitorDetailInfo {
  def this(id:Long,dt:String,mt:String,ct:String,ca:String,at:String,sd:String,ri:String) {
    this
    taskId= id
    date=dt
    monitorId = mt
    cameraId=ct
    car=ca
    actionTime=at
    speed=sd
    roadId=ri
  }

  private[this] var _taskId: Long = 0

  def taskId: Long = _taskId

  def taskId_=(value: Long): Unit = {
    _taskId = value
  }

  private[this] var _date: String = ""

  def date: String = _date

  def date_=(value: String): Unit = {
    _date = value
  }

  private[this] var _monitorId: String = ""

  def monitorId: String = _monitorId

  def monitorId_=(value: String): Unit = {
    _monitorId = value
  }

  private[this] var _cameraId: String = ""

  def cameraId: String = _cameraId

  def cameraId_=(value: String): Unit = {
    _cameraId = value
  }

  private[this] var _car: String = ""

  def car: String = _car

  def car_=(value: String): Unit = {
    _car = value
  }

  private[this] var _actionTime: String = ""

  def actionTime: String = _actionTime

  def actionTime_=(value: String): Unit = {
    _actionTime = value
  }

  private[this] var _speed: String = ""

  def speed: String = _speed

  def speed_=(value: String): Unit = {
    _speed = value
  }

  private[this] var _roadId: String = ""

  def roadId: String = _roadId

  def roadId_=(value: String): Unit = {
    _roadId = value
  }

  override def toString = s"TopNMonitorDetailInfo($taskId, $date, $monitorId, $cameraId, $car, $actionTime, $speed, $roadId)"
}
