package cn.qphone.spark.dao

import cn.qphone.spark.bean.Task

trait ITaskDAO {
  def findTaskById( taskId:Long):Task
}
