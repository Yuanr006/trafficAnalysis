package cn.qphone.spark.dao.factory

import cn.qphone.spark.dao.{ITaskDAO, TaskDAOImpl}

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/14 22:22
  * @Description: ${Description}
  */
object DAOFactory {
  def getTaskDAO():ITaskDAO = {
    new TaskDAOImpl()
  }
}
