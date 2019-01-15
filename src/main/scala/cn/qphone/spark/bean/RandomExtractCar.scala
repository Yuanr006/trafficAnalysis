package cn.qphone.spark.bean

class RandomExtractCar {
  def this(id:Long,dt:String,ca:String,dh:String) {
    this
    taskId= id
    date=dt
    car = ca
    dateHour=dh
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

  private[this] var _car: String = ""

  def car: String = _car

  def car_=(value: String): Unit = {
    _car = value
  }

  private[this] var _dateHour: String = ""

  def dateHour: String = _dateHour

  def dateHour_=(value: String): Unit = {
    _dateHour = value
  }

  override def toString = s"RandomExtractCar($taskId, $date, $car, $dateHour)"
}
