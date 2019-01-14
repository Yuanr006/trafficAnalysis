package cn.qphone.spark.bean

import scala.beans.BeanProperty

abstract class Area {
  @BeanProperty
  var areaId: String = null
  @BeanProperty
  var areaName: String = null
}
