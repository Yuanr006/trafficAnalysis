package cn.qphone

import cn.qphone.spark.MonitorOneStepConvertRateAnalyze.MonitorOneStepConvertRateAnalyze
import cn.qphone.spark.constant.Constants
import cn.qphone.spark.dao.ITaskDAO
import cn.qphone.spark.dao.impl.TaskDAOImpl
import cn.qphone.spark.util.DateUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import org.qphone.scala.Utils.ParamUtils

import scala.collection.mutable.ArrayBuffer

object AppTest {
    def main(args: Array[String]): Unit = {
//       val taskdao:ITaskDAO=new TaskDAOImpl
//        val task =taskdao.findTaskById(5)
//        val jsonobject= JSON.parseObject(task.taskParams)
//        val roadSplit=ParamUtils.getParam(jsonobject,Constants.PARAM_MONITOR_FLOW).split(",")
//        val splitArray:ArrayBuffer[String]=new ArrayBuffer[String]()
//        for(i<-0 until roadSplit.length ){
//          var tmpSplit=""
//          for(j<-0 to i ){
//          tmpSplit+= (if(j==0) roadSplit(j) else "->"+roadSplit(j))}
//          splitArray+=tmpSplit
//        }
//      splitArray

    //   print(MonitorOneStepConvertRateAnalyze.getSplitFlow(temp))
//       val temp: Array[String]=Array("1","2")
//       print( ParamUtils.getTaskIdFromArgs(temp,Constants.SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT))
      val t1 = "2000-01-01 00:00:7"

      println(DateUtils.getSecondsByTime(t1))

    }



}


