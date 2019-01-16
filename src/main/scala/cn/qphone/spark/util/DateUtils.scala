package cn.qphone.spark.util
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

/**
  * 日期时间工具类
  */
object DateUtils {
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")
  val MINUTE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    print(getRangeTime("2018-1-1 1:59"))

  }

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def before(time1: String, time2: String): Boolean = {
    val dateTime1 = TIME_FORMAT.parse(time1)
    val dateTime2 = TIME_FORMAT.parse(time2)
    if (dateTime1.before(dateTime2)) {
      true
    } else {
      false
    }
  }

  /**
    * 判断一个时间是否在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def after(time1: String, time2: String): Boolean = {
    var t1 = time1
    var t2 = time2
    if (t1 == null || t2 == null || t1.equals("null") || t2.equals("null") || t1.equals("") || t2.equals("")) {
      t1 = "2000-01-01 00:00:00"
      t2 = "2000-01-01 00:00:00"
    }
    val dateTime1 = TIME_FORMAT.parse(t1)
    val dateTime2 = TIME_FORMAT.parse(t2)
    if (dateTime1.after(dateTime2)) {
      true
    } else {
      false
    }
  }

  /**
    * 计算时间差值（单位为秒）
    *
    * @param time1 时间1
    * @param time2 时间2
    * @return 差值
    */
  def minus(time1: String, time2: String): Int = {
    var t1 = time1
    var t2 = time2
    if (t1 == null || t2 == null || t1.equals("null") || t2.equals("null") || t1.equals("") || t2.equals("")) {
      t1 = "2000-01-01 00:00:00"
      t2 = "2000-01-01 00:00:00"
    }
    try {
      val datetime1 = TIME_FORMAT.parse(t1)
      val datetime2 = TIME_FORMAT.parse(t2)
      val millisecond = datetime1.getTime - datetime2.getTime
      Integer.valueOf(String.valueOf(millisecond / 1000))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        0
    }


  }

  /**
    * 获取年月日和小时
    *
    * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
    * @return 结果（yyyy-MM-dd_HH）
    */
  def getDateHour(datetime: String): String = {
    var t1 = datetime
    if (t1 == null || t1.equals("null") || t1.equals("")) {
      t1 = "2000-01-01 00:00:00"
    }
    val date = t1.split(" ")(0)
    val hourMinuteSecond = t1.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

  /**
    * 获取当天日期（yyyy-MM-dd）
    *
    * @return 当天日期
    */
  def getTodayDate: String = {
    DATE_FORMAT.format(System.currentTimeMillis())
  }

  /**
    * 获取昨天的日期（yyyy-MM-dd）
    *
    * @return 昨天的日期
    */

  import java.util.Calendar

  def getYesterdayDate: String = {
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.add(Calendar.DAY_OF_YEAR, -1)
    val date = cal.getTime
    DATE_FORMAT.format(date)
  }

  /**
    * 格式化日期（yyyy-MM-dd）
    *
    * @param date Date对象
    * @return 格式化后的日期
    */

  def formatDate(date: Date): String = {
    DATE_FORMAT.format(date)
  }

  /**
    * 格式化时间（yyyy-MM-dd HH:mm:ss）
    *
    * @param date Date对象
    * @return 格式化后的时间
    */

  def formatTime(date: Date): String = {
    TIME_FORMAT.format(date)
  }


  /**
    * 解析时间字符串
    *
    * @param time 时间字符串
    * @return Date
    */

  def parseTime(time: String): Date = {
    try {
      TIME_FORMAT.parse(time)
    }
    catch {
      case e: Exception =>
        e.printStackTrace
        null
    }

  }


  /**
    * 格式化日期key
    *
    * @param date
    * @return yyyyMMdd
    */


  def formatDateKey(date: Date): String = DATEKEY_FORMAT.format(date)

  /**
    * 格式化日期key	(yyyyMMdd)
    *
    * @param datekey
    * @return
    */


  def parseDateKey(datekey: String): Date = {
    var t1 = datekey
    if (t1 == null || t1.equals("null") || t1.equals("")) {
      t1 = "2000-01-01 00:00:00"
    }
    try {
      DATEKEY_FORMAT.parse(t1)
    } catch {
      case e: Exception =>
        e.printStackTrace
        null
    }

  }

  import java.text.SimpleDateFormat

  /**
    * 格式化时间，保留到分钟级别
    *
    * @param date
    * @return yyyyMMddHHmm --201701012301
    */
  def formatTimeMinute(date: Date): String = {

    MINUTE_FORMAT.format(date)
  }

  /**
    *
    * @param dateTime yyyy-MM-dd HH:mm:ss
    * @return
    */


  def getRangeTime(dateTime: String): String = {
    val date = dateTime.split(" ")(0)
    val hour = dateTime.split(" ")(1).split(":")(0)
    val minute = StringUtils.convertStringtoInt(dateTime.split(" ")(1).split(":")(1))
    if (minute + (5 - minute % 5) == 60) {
      date + " " + hour + ":" + StringUtils.fulfuill((minute - (minute % 5)) + "") +
        "~" + date + " " + StringUtils.fulfuill((hour.toInt + 1) + "") + ":00"
    }
    date + " " + hour + ":" + StringUtils.fulfuill((minute - (minute % 5)) + "") +
      "~" + date + " " + hour + ":" + StringUtils.fulfuill((minute + (5 - minute % 5)) + "")
  }
  /**
    *
    * @param dateTime yyyy-MM-dd HH:mm:ss
    * @return
    */
  def time2Stamp(time:String):Long={
   1L
  }
  //获取指定日期的秒
 def getSecondsByTime( time:String):Long = {
    LocalDateTime.parse(time,formatter)
      .atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();

  }

}
