package cn.qphone.spark.util

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/15 09:45
  * @Description: 字符串格式化累
  */
object StringUtils {
  //delimter "\\|"
  /**
    * 判断字符串是否为空
    *
    * @param str 字符串
    * @return 是否为空
    */

  def isEmpty(str: String): Boolean = {
    str == null || "" == str
  }

  /**
    * 判断字符串是否不为空
    *
    * @param str 字符串
    * @return 是否不为空
    */
  def isNotEmpty(str: String): Boolean = {
    str != null && !("" == str)
  }

  /**
    * 截断字符串两侧的逗号
    *
    * @param str 字符串
    * @return 字符串
    */

  def trimComma(str: String): String = {
    var s = ""
    if (str.startsWith(",")) {
      s = str.substring(1)
    }
    if (str.endsWith(",")) {
      s = str.substring(0, str.length - 1)
    }
    s
  }

  /**
    * 补全两位数字
    *
    * @param str
    * @return
    */
  def fulfuill(str: String): String = {
    if (str.length == 1) {
      "0" + str
    }
    str
  }

  /**
    * 补全num位数字
    * 将给定的字符串前面补0，使字符串的长度为num位。
    *
    * @param str
    * @return
    */
  def fulfuill(num: Int, str: String): String = {
    if (str.length == num) str
    else {
      val fulNum = num - str.length
      var tmpStr = ""
      var i = 0
      while ( {
        i < fulNum
      }) {
        tmpStr += "0"

        {
          i += 1;
          i - 1
        }
      }
      tmpStr + str
    }
  }

  /**
    * 从拼接的字符串中提取字段
    *
    * @param str       字符串
    * @param delimiter 分隔符
    * @param field     字段
    * @return 字段值
    *         name=zhangsan|age=18
    */
  def getFieldFromConcatString(str: String, delimiter: String, field: String): String = {
    val fields = str.split(delimiter)
    var fieldValue = ""
    for (concatField <- fields) {
      if (concatField.split("=").length == 2) {
        val fieldName = concatField.split("=")(0)
        fieldValue = concatField.split("=")(1)
        if (fieldName == field) fieldValue
      }
    }
    fieldValue
  }


  /**
    * 从拼接的字符串中给字段设置值
    *
    * @param str           字符串
    * @param delimiter     分隔符
    * @param field         字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    *         name=zhangsan|age=12
    *         |
    *         age
    *         18
    *         name=zhangsan|age=18
    */
  def setFieldInConcatString(str: String, delimiter: String, field: String, newFieldValue: String): String = {
    val fields = str.split(delimiter)
    for (i <- 0 to fields.length - 1) {
      val fieldStr = fields(i).split("=")
      val fieldName = fieldStr(0)
      if (fieldName.equals(field)) {
        val concatField = fieldName + "=" + newFieldValue
        fields(i) = concatField
      }
    }
    val buffer = new StringBuffer("")
    for (i <- 0 to fields.length - 1) {
      buffer.append(fields(i))
      if (i < fields.length - 1) buffer.append("|")
    }
    buffer.toString
  }

  /**
    * 给定字符串和分隔符，返回一个K,V map
    * name=zhangsan|age=18
    *
    * @param str
    * @param delimiter
    * @return map<String,String>
    *
    */
  def getKeyValuesFromConcatString(str: String, delimiter: String): Map[String, String] = {
    var map: Map[String, String] = Map()
    val fields = str.split(delimiter)
    for (concatField <- fields) {
      if (concatField.split("=").length == 2) {
        val fieldName = concatField.split("=")(0)
        val fieldValue = concatField.split("=")(1)
        map.getOrElse(fieldName, fieldValue)
      }
    }
    map
  }

  /**
    * String 字符串转Integer数字
    *
    * @param str
    * @return
    */
  def convertStringtoInt(str: String): Integer = {
    str.toInt
  }
}
