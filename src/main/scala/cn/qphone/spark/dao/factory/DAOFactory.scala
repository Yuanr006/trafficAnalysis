package cn.qphone.spark.dao.factory

import cn.qphone.spark.dao.impl.{MonitorDAOImpl, TaskDAOImpl}
import cn.qphone.spark.dao.{IMonitorDAO, ITaskDAO}

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/14 22:22
  * @Description: ${Description}
  */
object DAOFactory {
  def getTaskDAO():ITaskDAO = {
    new TaskDAOImpl()
  }
  def getMonitorDAO():IMonitorDAO={
    new MonitorDAOImpl()
  }
}
