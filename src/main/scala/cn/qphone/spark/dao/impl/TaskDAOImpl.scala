package cn.qphone.spark.dao

import java.sql.ResultSet

import cn.qphone.spark.bean.{RandomExtractCar, RandomExtractMonitorDetail, Task}
import cn.qphone.spark.jdbc.JDBCHelper
import cn.qphone.spark.jdbc.JDBCHelper.QueryCallback

object TaskDAOImpl extends ITaskDAO {
  override def findTaskById(taskId: Long): Task = {
    val task = new Task();

    val sql = "SELECT * FROM task WHERE task_id = ?";

    val params = Array[String](taskId.toString)

    JDBCHelper.executeQuery(sql, params, new QueryCallback() {

      @Override
      def process(rs:ResultSet) {
        if(rs.next()) {
          val taskid = rs.getLong(1)
          val taskName = rs.getString(2)
          val createTime = rs.getString(3)
          val startTime = rs.getString(4)
          val finishTime = rs.getString(5)
          val taskType = rs.getString(6)
          val taskStatus = rs.getString(7)
          val taskParam = rs.getString(8)

          task.taskId_=(taskid)
          task.taskName_=(taskName)
          task.createTime_=(createTime)
          task.startTime_=(startTime)
          task.finishTime_=(finishTime)
          task.taskType_=(taskType)
          task.taskStatus_=(taskStatus)
          task.taskParams_=(taskParam)
        }
      }
    })
    task
  }
}
