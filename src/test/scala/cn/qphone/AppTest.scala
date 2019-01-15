package cn.qphone

import cn.qphone.spark.constant.Constants
import cn.qphone.spark.dao.ITaskDAO
import cn.qphone.spark.dao.impl.TaskDAOImpl
import com.alibaba.fastjson.{JSON, JSONObject}
import org.qphone.scala.Utils.ParamUtils

object AppTest {
    def main(args: Array[String]): Unit = {
//        val taskdao:ITaskDAO=new TaskDAOImpl
//        val task =taskdao.findTaskById(1)
//        val jsonobject= JSON.parseObject(task.taskParams)
//        print(ParamUtils.getParam(jsonobject,Constants.PARAM_START_DATE))
       val temp: Array[String]=Array("1","2")
       print( ParamUtils.getTaskIdFromArgs(temp,Constants.SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT))

    }



}


