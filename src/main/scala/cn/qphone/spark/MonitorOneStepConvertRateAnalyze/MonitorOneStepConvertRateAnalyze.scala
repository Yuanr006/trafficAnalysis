package cn.qphone.spark.MonitorOneStepConvertRateAnalyze

import cn.qphone.spark.constant.Constants
import cn.qphone.spark.dao.ITaskDAO
import cn.qphone.spark.dao.impl.TaskDAOImpl
import cn.qphone.spark.mockData.MockData
import cn.qphone.spark.util.{DateUtils, NumberUtils, ParamUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/15 20:19
  * @Description: 卡扣车流量转化率 Spark Core
  */
object MonitorOneStepConvertRateAnalyze {
  /**
    * 卡扣流
    * monitor_id   1 2 3 4      1_2 2_3 3_4
    * 指定一个道路流  1 2 3 4
    * 1 carCount1 2carCount2  转化率 carCount2/carCount1
    * 1 2 3  转化率                           1 2 3的车流量/1 2的车流量
    * 1 2 3 4 转化率       1 2 3 4的车流量 / 1 2 3 的车流量
    * 京A1234	1,2,3,6,2,3
    * 1、查询出来的数据封装到cameraRDD
    * 2、计算每一车的轨迹
    * 3、匹配指定的道路流       1：carCount   1，2：carCount   1,2,3carCount
    */
  def main(args: Array[String]): Unit = {
    // 1、构造Spark上下文
    val sparkConf: SparkConf = new SparkConf
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("mockData")
    System.setProperty("HADOOP_USER_NAME", "root")
    val sc = new SparkContext(sparkConf)
    //设置日志的级别
    sc.setLogLevel("WARN")
    val sqlContext: SQLContext = new SQLContext(sc)
    // 2、生成模拟数据
    MockData.mock(sc, sqlContext)
    // 3、查询任务，获取任务的参数
    val taskId: Long = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT)
    val taskdao: ITaskDAO = new TaskDAOImpl
    val task = taskdao.findTaskById(taskId)

    if (task == null) {
      System.exit(-1)
    }
    val taskParam = JSON.parseObject(task.taskParams)
    /**
      * 从数据库中查找出来我们指定的卡扣流
      * 0001,0002,0003,0004,0005
      */
    val roadFlow = ParamUtils.getParam(taskParam, Constants.PARAM_MONITOR_FLOW)
    val roadFlowBroadcast = sc.broadcast(getSplitFlow(roadFlow))
    /**
      * 通过时间的范围拿到合法的车辆
      */
    val rowRDDByDateRange = getCameraRDDByDateRange(sqlContext, taskParam)
    /**
      * 将rowRDDByDateRange 变成key-value对的形式，key car value 详细信息
      * （key,row）
      * 为什么要变成k v对的形式？
      * 因为下面要对car 按照时间排序，绘制出这辆车的轨迹。
      *
      */
    val carTracker = getPerCarMonitors(rowRDDByDateRange)
   // print("车辆流信息")
   print(carTracker.count())
    val roadFlow2Count = matchRowSplit(carTracker, roadFlowBroadcast, sc)
 //   roadFlow2Count.foreach(println)
    val rateMap = computeRoadSplitConvertRate(roadFlow2Count, roadFlow);
    rateMap.foreach(println)

    sc.stop()

  }

  /**
    * 查询指定日期的数据
    *
    * @param sqlContext
    * @param taskParam
    */
  def getCameraRDDByDateRange(sqlContext: SQLContext, taskParam: JSONObject): RDD[Row] = {

    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "SELECT * FROM monitor_flow_action WHERE date>='" + startDate + "' AND date<='" + endDate + "'"
    sqlContext.sql(sql).rdd

    /**
      * repartition可以提高stage的并行度
      */
    //.repartition(1000)
  }
//: Map[String, Long]
  def matchRowSplit(carTracker: RDD[String], roadFlowBroadcast: Broadcast[ArrayBuffer[String]], sc: SparkContext)= {

    val flowArray = roadFlowBroadcast.value


  // carTracker.foreach(println)
  val list=carTracker.map(str => {
      val list: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]
      for (flow <- flowArray) {
        //indexOf 从哪个位置开始查找
        var index = 0
        //这辆车有多少次匹配到这个卡扣切片的次数
        var count = 0L
        while (str.indexOf(flow, index) != -1) {
          index = str.indexOf(flow, index) + 1
          count = count + 1
        }
        list += ((flow, count))
      }
      list
    }).fold(new ListBuffer[(String, Long)])((l1,l2)=>{l1.appendAll(l2);l1}).toList

   sc.parallelize(list).groupByKey().map(e=>{(e._1,e._2.sum)}).collect().toMap



  }

  //
  def getPerCarMonitors(rowRDDByDateRange: RDD[Row]): RDD[String] = {
    rowRDDByDateRange.map(elem => {

      (elem.getString(3), (DateUtils.getSecondsByTime(elem.getString(4)), elem.getString(1)))
    }).groupByKey().map(elem => {
      //生成按时间排序的卡扣字符串0003, 0006, 0002, 0007, 0005, 0007, 0002
      val str: StringBuffer = new StringBuffer();
      elem._2.toList.sortBy(_._1).foreach(e => str.append("," + e._2))
      str.substring(1)
    })
  }

  /**
    * 获取参数中的切片
    *
    * @param roadFlow
    * @return
    */
  def getSplitFlow(roadFlow: String): ArrayBuffer[String] = {
    /**
      * 从广播变量中获取指定的卡扣流参数
      * 0001,0002,0003,0004,0005
      */
    /**
      * 对指定的卡扣流参数分割
      * 0001
      * 0001->0002
      * 0001->0002->0003
      * 0001->0002->0003->0004
      * 0001->0002->0003->0004->0005
      */
    val roadSplit = roadFlow.split(",", -1)
    val splitArray: ArrayBuffer[String] = new ArrayBuffer[String]()
    for (i <- 0 until roadSplit.length) {
      var tmpSplit = ""
      for (j <- 0 to i) {
        tmpSplit += (if (j == 0) roadSplit(j) else "," + roadSplit(j))
      }
      splitArray += tmpSplit
    }
    splitArray

  }

  def computeRoadSplitConvertRate(roadFlow2Count: Map[String, Long], roadFlow: String) = {
    val rateMap: mutable.HashMap[String, Double] = new mutable.HashMap[String, Double]()
    var lastMonitorCarCount = 0L
    var tmpRoadFlow = ""
    val split = roadFlow.split(",")

    for (i <- 0 until split.length) {
      tmpRoadFlow += "," + split(i)
      val count: Long = roadFlow2Count.getOrElse(tmpRoadFlow.substring(1), 0)

      if (count != 0L) {


        /**
          * 1_2
          * lastMonitorCarCount      1 count
          */
        if (i != 0 && lastMonitorCarCount != 0L) {
          val rate = NumberUtils.formatDouble(count.toDouble / lastMonitorCarCount.toDouble, 2)
          rateMap += ((tmpRoadFlow.substring(1), rate))
        }
        lastMonitorCarCount = count
      }


    }
    rateMap
  }
}
