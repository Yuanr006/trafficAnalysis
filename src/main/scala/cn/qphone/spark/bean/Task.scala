package cn.qphone.spark.bean


class Task {
  def this(id:Long,tn:String,ct:String,st:String,ft:String,tt:String,ts:String,tp:String) {
    this
    taskId= id
    taskName=tn
    createTime = ct
    startTime=st
    finishTime=ft
    taskType=tt
    taskStatus=ts
    taskParams=tp
  }

  private[this] var _taskId: Long = 0

  def taskId: Long = _taskId

  def taskId_=(value: Long): Unit = {
    _taskId = value
  }

  private[this] var _taskName: String = ""

  def taskName: String = _taskName

  def taskName_=(value: String): Unit = {
    _taskName = value
  }

  private[this] var _createTime: String = ""

  def createTime: String = _createTime

  def createTime_=(value: String): Unit = {
    _createTime = value
  }

  private[this] var _startTime: String = ""

  def startTime: String = _startTime

  def startTime_=(value: String): Unit = {
    _startTime = value
  }

  private[this] var _finishTime: String = ""

  def finishTime: String = _finishTime

  def finishTime_=(value: String): Unit = {
    _finishTime = value
  }

  private[this] var _taskType: String = ""

  def taskType: String = _taskType

  def taskType_=(value: String): Unit = {
    _taskType = value
  }

  private[this] var _taskStatus: String = ""

  def taskStatus: String = _taskStatus

  def taskStatus_=(value: String): Unit = {
    _taskStatus = value
  }

  private[this] var _taskParams: String = ""

  def taskParams: String = _taskParams

  def taskParams_=(value: String): Unit = {
    _taskParams = value
  }

  override def toString = s"Task($taskId, $taskName, $createTime, $startTime, $finishTime, $taskType, $taskStatus, $taskParams)"
}
