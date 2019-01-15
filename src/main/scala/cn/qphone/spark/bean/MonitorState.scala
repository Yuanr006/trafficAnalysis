package cn.qphone.spark.bean

class MonitorState {
  def this(id:Long,nmc:String,ncc:String,amc:String,acc:String,amci:String) {
    this
    taskId= id
    normalMonitorCount=nmc
    normalCameraCount = ncc
    abnormalMonitorCount=amc
    abnormalCameraCount=acc
    abnormalMonitorCameraInfos=amci
  }

  private[this] var _taskId: Long = 0

  def taskId: Long = _taskId

  def taskId_=(value: Long): Unit = {
    _taskId = value
  }

  //正常的卡扣个数
  private[this] var _normalMonitorCount: String = ""

  def normalMonitorCount: String = _normalMonitorCount

  def normalMonitorCount_=(value: String): Unit = {
    _normalMonitorCount = value
  }

  //正常的摄像头个数
  private[this] var _normalCameraCount: String = ""

  def normalCameraCount: String = _normalCameraCount

  def normalCameraCount_=(value: String): Unit = {
    _normalCameraCount = value
  }

  //不正常的卡扣个数
  private[this] var _abnormalMonitorCount: String = ""

  def abnormalMonitorCount: String = _abnormalMonitorCount

  def abnormalMonitorCount_=(value: String): Unit = {
    _abnormalMonitorCount = value
  }

  //不正常的摄像头个数
  private[this] var _abnormalCameraCount: String = ""

  def abnormalCameraCount: String = _abnormalCameraCount

  def abnormalCameraCount_=(value: String): Unit = {
    _abnormalCameraCount = value
  }

  //不正常的摄像头详细信息
  private[this] var _abnormalMonitorCameraInfos: String = null

  def abnormalMonitorCameraInfos: String = _abnormalMonitorCameraInfos

  def abnormalMonitorCameraInfos_=(value: String): Unit = {
    _abnormalMonitorCameraInfos = value
  }

  override def toString = s"MonitorState($taskId, $normalMonitorCount, $normalCameraCount, $abnormalMonitorCount, $abnormalCameraCount, $abnormalMonitorCameraInfos)"
}
