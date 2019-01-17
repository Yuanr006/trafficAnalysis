package cn.qphone.spark.skynet

import java.text.SimpleDateFormat

import cn.qphone.spark.bean.{MonitorState, TopNMonitor2CarCount, TopNMonitorDetailInfo}
import cn.qphone.spark.constant.Constants
import cn.qphone.spark.dao.factory.DAOFactory
import cn.qphone.spark.mockData.MockData
import cn.qphone.spark.util.ParamUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object MonitorFlowAnalyze{
  /**
    * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的表就可以
    * 本地模拟数据注册成一张临时表
    * monitor_flow_action	数据表：监控车流量所有数据
    * monitor_camera_info	标准表：卡扣对应摄像头标准表
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    MockData.mock(sc, sqlContext)
    /**
      * 从配置文件my.properties中拿到spark.local.taskId.monitorFlow的taskId
      */
    val taskId = 1
    /**
      * 获取ITaskDAO的对象，通过taskId查询出来的数据封装到Task（自定义）对象
      */
    val taskDAO = DAOFactory.getTaskDAO()
    val task = taskDAO.findTaskById(taskId)
    val taskParamsJsonObject = JSON.parseObject(task.taskParams)
//    monitorState(sqlContext,sc,taskParamsJsonObject,taskId)
//    topnMonitorCarCount(sqlContext,sc,taskParamsJsonObject,taskId)
//    topnMonitorDetailInfo(sqlContext,sc,taskParamsJsonObject,taskId)
//    areaCarPeng(sqlContext,sc,taskParamsJsonObject,taskId)
    CarPeng(sqlContext,sc,taskParamsJsonObject,taskId,"01","02")
  }
  def monitorState(sqlContext:SQLContext,sc:SparkContext,taskParamsJsonObject:JSONObject,taskId:Int): Unit ={
    /**
      * 给定一个时间段，统计出卡口数量的正常数量，异常数量，还有通道数
      * 异常数：每一个卡口都会有n个摄像头对应每一个车道，
      * 如果这一段时间内卡口的信息没有第N车道的信息的话就说明这个卡口存在异常。
      * 这需要拿到一份数据（每一个卡口对应的摄像头的编号）
      * 模拟数据在monitor_camera_info临时表中
      */
    /**
      * task.getTaskParams()是一个json格式的字符串   封装到taskParamsJsonObject
      * 将 task_parm字符串转换成json格式数据。
      */
    val startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE)
    val sql = "SELECT * FROM monitor_flow_action where date>='" + startDate + "' " + "AND date<='" + endDate + "'"
    val sql2 = "SELECT * FROM monitor_camera_info "
    val rdd = sqlContext.sql(sql2).rdd.map(x => (x.get(0), x.get(1))).groupByKey()
    val rdd2 = sqlContext.sql(sql).rdd.map(x => (x.get(1), x.get(2))).groupByKey()
    val noraml_monitor_count = sc.longAccumulator
    val normal_camera_count = sc.longAccumulator
    val abnormal_monitor_count = sc.longAccumulator
    val abnormal_camera_count = sc.longAccumulator
    val acmi = new MonitorAndCameraStateAccumulator
    sc.register(acmi,"myAcc")
    rdd2.leftOuterJoin(rdd).map(x => {
      val list1 = x._2._1.toList
      val buffer = ListBuffer[String]()
      var flag = true
      var count = 0
      for (elem <- x._2._2.get.toList) {
        if (!list1.contains(elem)) {
          buffer.append(elem.toString)
          abnormal_camera_count.add(1)
          count += 1
          flag = false
        } else {
          normal_camera_count.add(1)
        }
      }
      if (flag != false) {
        noraml_monitor_count.add(1)
      } else {
        abnormal_monitor_count.add(1)
      }
      (x._1, buffer.mkString(",")+"|count:"+count)
    }).foreach(x=>{
      acmi.add(x._1+":"+x._2)
    })
    saveMonitorState(taskId,noraml_monitor_count.value,normal_camera_count.value,abnormal_monitor_count.value
      ,abnormal_camera_count.value,acmi.value.substring(0,acmi.value.length-1))
  }
  def saveMonitorState(taskId:Long,nmc:Long,ncc:Long,amc:Long,acc:Long,acmi:String): Unit ={
    val monitorState = new MonitorState(taskId,nmc.toString,ncc.toString,amc.toString,acc.toString,acmi)
    val monitorDAO = DAOFactory.getMonitorDAO()
    monitorDAO.insertMonitorState(monitorState)
  }
  def topnMonitorCarCount(sqlContext:SQLContext,sc:SparkContext,taskParamsJsonObject:JSONObject,taskId:Int): Unit ={
    val topnnum = ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_TOP_NUM).toInt
    val sql = "SELECT * FROM monitor_flow_action"
    val rdd1 = sqlContext.sql(sql).rdd
    val topn_monitorcarcount = rdd1.map(x=> (x.get(1),x)).groupByKey().map(x=>(x._2.size,x._1.toString)).top(topnnum).map(x=>(x._2,x._1))
    val array =ArrayBuffer[TopNMonitor2CarCount]()
    topn_monitorcarcount.foreach(x=>{
      val topNMonitor2CarCount = new TopNMonitor2CarCount(taskId,x._1,x._2.toString)
      array.append(topNMonitor2CarCount)
    })
    saveTopnMonitorCarCount(array)
  }
  def saveTopnMonitorCarCount(topn_monitorcarcount:ArrayBuffer[TopNMonitor2CarCount]): Unit ={
    val monitorDAO = DAOFactory.getMonitorDAO()
    monitorDAO.insertBatchTopN(topn_monitorcarcount.toList)
  }
  def topnMonitorDetailInfo(sqlContext:SQLContext,sc:SparkContext,taskParamsJsonObject:JSONObject,taskId:Int): Unit ={
    val topnnum = ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_TOP_NUM).toInt
    val sql = "SELECT * FROM monitor_flow_action where speed>120"
    val rdd1 = sqlContext.sql(sql).rdd.map(x=>(x.getString(1),x)).groupByKey().sortBy(_._2.size).take(topnnum)
    val topnbuffer = ArrayBuffer[Row]()
    val top10buffer = ArrayBuffer[Row]()
    for (elem <- rdd1) {
      val buffer1 = ArrayBuffer[(Int,Row)]()
      for (elem <- elem._2) {
        topnbuffer.append(elem)
        buffer1.append((elem.getString(5).toInt,elem))
      }
      buffer1.sortBy(_._1).reverse.take(10).map(_._2).foreach(top10buffer.append(_))
    }
    val top10list = ListBuffer[TopNMonitorDetailInfo]()
    val topnlist = ListBuffer[TopNMonitorDetailInfo]()
    for (elem <- topnbuffer) {
      topnlist.append(new TopNMonitorDetailInfo(taskId,elem.getString(0),elem.getString(1),elem.getString(2)
        ,elem.getString(3),elem.getString(4),elem.getString(5),elem.getString(6)))
    }
    for (elem <- top10buffer) {
      top10list.append(new TopNMonitorDetailInfo(taskId,elem.getString(0),elem.getString(1),elem.getString(2)
        ,elem.getString(3),elem.getString(4),elem.getString(5),elem.getString(6)))
    }
    saveTopnMonitorDetailInfo(topnlist)
    saveTop10SpeedDetaileInfo(top10list)
  }
  def saveTopnMonitorDetailInfo(list:ListBuffer[TopNMonitorDetailInfo]): Unit ={
    val monitorDAO = DAOFactory.getMonitorDAO()
    monitorDAO.insertBatchMonitorDetails(list.toList)
  }
  def saveTop10SpeedDetaileInfo(list:ListBuffer[TopNMonitorDetailInfo]): Unit ={
    val monitorDAO = DAOFactory.getMonitorDAO()
    monitorDAO.insertBatchTop10Details(list.toList)
  }
  def areaCarPeng(sqlContext:SQLContext,sc:SparkContext,taskParamsJsonObject:JSONObject,taskId:Int): Unit = {
    val monitorIds1 = List("0000", "0001", "0002", "0003")
    val monitorIds2 = List("0004", "0005", "0006", "0007")
    val startTime = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE);
    val endTime = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE);
    val rdd1 = sqlContext.sql(jointsql(monitorIds1,startTime,endTime)).rdd.map(_.getString(3)).distinct()
    val rdd2 = sqlContext.sql(jointsql(monitorIds2,startTime,endTime)).rdd.map(_.getString(3)).distinct()
    rdd1.intersection(rdd2).foreach(println)
  }
  def jointsql(monitorIds1:List[String],startTime:String,endTime:String): String ={
    var sql = "SELECT * "+ "FROM monitor_flow_action"+ " WHERE date >='" + startTime + "' "+ " AND date <= '" + endTime+ "' "+ " AND monitor_id in ("
    for( i <- 0 until monitorIds1.length){
      sql += "'"+monitorIds1(i) + "'"
      if( i  < monitorIds1.length - 1 ){
        sql += ","
      }
    }
    sql += ")"
    sql
  }
  def CarPeng(sqlContext:SQLContext,sc:SparkContext,taskParamsJsonObject:JSONObject,taskId:Int,area1:String,area2:String): Unit ={
    val startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE)
    val sql1 = "SELECT * "+ "FROM monitor_flow_action "+ "WHERE date>='" + startDate + "' "+ "AND date<='" + endDate + "'"+ "AND area_id in ('"+area1 +"')"
    val sql2 = "SELECT * "+ "FROM monitor_flow_action "+ "WHERE date>='" + startDate + "' "+ "AND date<='" + endDate + "'"+ "AND area_id in ('"+area2 +"')"
    val rdd1 = sqlContext.sql(sql1).rdd.map(x=>(x.getString(3),x)).groupByKey()
    val rdd2 = sqlContext.sql(sql2).rdd.map(x=>(x.getString(3),x)).groupByKey()
    rdd1.join(rdd2).map(x=>{
      val arr1 = x._2._1.toArray
      val arr2 = x._2._2.toArray
      val buff = ListBuffer[(Row,Row)]()
      for (i <-0 until arr1.length) {
        val timestamp1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr1(i).getString(4)).getTime
        for (j <-0 until arr2.length) {
          val timestamp2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr2(j).getString(4)).getTime
          if((timestamp1-timestamp2).abs<=3600000){
            buff.append((arr1(i),arr2(j)))
          }
        }
      }
      buff
    }).saveAsTextFile("e://opt")
  }
}
