package cn.qphone.spark.bean

class TopNMonitor2CarCount {
  def this(id:Long,mi:String,cc:String) {
    this
    taskId= id
    monitorId=mi
    carCount = cc
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

  private[this] var _carCount: String = ""

  def carCount: String = _carCount

  def carCount_=(value: String): Unit = {
    _carCount = value
  }

  override def toString = s"TopNMonitor2CarCount($taskId, $monitorId, $carCount)"
}
