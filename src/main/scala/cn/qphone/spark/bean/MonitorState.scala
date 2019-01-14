package cn.qphone.spark.bean

import scala.beans.BeanProperty

class MonitorState {
  @BeanProperty
  var taskId: String = null
  @BeanProperty
  var normalMonitorCount: String = null //正常的卡扣个数
  @BeanProperty
  var normalCameraCount: String = null //正常的摄像头个数
  @BeanProperty
  var abnormalMonitorCount :String= null //不正常的卡扣个数
  @BeanProperty
  var abnormalCameraCount :String= null //不正常的摄像头个数
  @BeanProperty
  var abnormalMonitorCameraInfos :String= null //不正常的摄像头详细信息

}
