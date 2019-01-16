package cn.qphone.spark.util

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDate, LocalDateTime, Period, ZoneId}
import java.util.Date

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/16 16:13
  * @Description: ${Description}
  */
object LocalDateTimeUtils {

  //  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  //获取当前时间的LocalDateTime对象
  //LocalDateTime.now()

  //根据年月日构建LocalDateTime
  //LocalDateTime.of()

  //比较日期先后
  //LocalDateTime.now().isBefore(),
  //LocalDateTime.now().isAfter(),

  //Date转换为LocalDateTime
  def convertDateToLDT(date: Date): LocalDateTime = {
    LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault())
  }

  //LocalDateTime转换为Date
  def convertLDTToDate(time: LocalDateTime): Date = {
    Date.from(time.atZone(ZoneId.systemDefault()).toInstant())
  }


  //获取指定日期的毫秒
  def getMilliByTime(time: LocalDateTime): Long = {
    time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
  }

  //获取指定日期的秒
  def getSecondsByTime(time: LocalDateTime): Long = {
    time.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond()
  }

  //获取指定时间的指定格式
  def formatTime(time: LocalDateTime, pattern: String): String = {
    time.format(DateTimeFormatter.ofPattern(pattern))
  }

  //获取当前时间的指定格式
  def formatNow(pattern: String): String = {
    formatTime(LocalDateTime.now(), pattern)
  }

  //日期加上一个数,根据field不同加不同值,field为ChronoUnit.*
  def plus(time: LocalDateTime, number: Long, field: TemporalUnit): LocalDateTime = {
    time.plus(number, field)
  }

  //日期减去一个数,根据field不同减不同值,field参数为ChronoUnit.*
  def minu(time: LocalDateTime, number: Long, field: TemporalUnit): LocalDateTime = {
    time.minus(number, field)
  }

  /**
    * 获取两个日期的差  field参数为ChronoUnit.*
    *
    * @param startTime
    * @param endTime
    * @param field 单位(年月日时分秒)
    *              @
    */
  def betweenTwoTime(startTime: LocalDateTime, endTime: LocalDateTime, field: ChronoUnit): Long = {
    val period = Period.between(LocalDate.from(startTime), LocalDate.from(endTime))
    if (field == ChronoUnit.YEARS) period.getYears()
    if (field == ChronoUnit.MONTHS) period.getYears() * 12 + period.getMonths()
    field.between(startTime, endTime)
  }

  //获取一天的开始时间，2017,7,22 00:00
  def getDayStart(time: LocalDateTime): LocalDateTime = {
    time.withHour(0)
      .withMinute(0)
      .withSecond(0)
      .withNano(0)
  }

  //获取一天的结束时间，2017,7,22 23:59:59.999999999
  def getDayEnd(time: LocalDateTime): LocalDateTime = {
    time.withHour(23)
      .withMinute(59)
      .withSecond(59)
      .withNano(999999999)
  }


}
