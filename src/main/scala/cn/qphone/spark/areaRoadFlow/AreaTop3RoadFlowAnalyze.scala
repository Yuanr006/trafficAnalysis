package cn.qphone.spark.areaRoadFlow

import cn.qphone.spark.bean.Task
import cn.qphone.spark.conf.ConfigurationManager
import cn.qphone.spark.constant.Constants
import cn.qphone.spark.dao.ITaskDAO
import cn.qphone.spark.dao.factory.DAOFactory
import cn.qphone.spark.mockData.{MockData, Mockdata}
import cn.qphone.spark.util.{ParamUtils, SparkUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.qphone.scala.Utils.ParamUtils

import scala.util.Random

/**
  * 计算出每一个区域top3的道路流量
  * 每一个区域车流量最多的3条道路   每条道路有多个卡扣
  * 这是一个分组取topN  SparkSQL分组取topN
  * 区域，道路流量排序         按照区域和道路进行分组
  *
  */
object AreaTop3RoadFlowAnalyze  {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AreaTop3ProductSpark").set("spark.sql.shuffle.partitions", "1").setMaster("local[*]")
    System.setProperty("HADOOP_USER_NAME", "root")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.udf.register("ConcatStringStringUDF", (area_name: String, road_id: String, split: String) => {
      area_name + split + road_id
    })
    sqlContext.udf.register("random_prefix", (area_name_road_id: String, ranNum: Integer) => {
      (new Random).nextInt(ranNum) + "_" + area_name_road_id
    })
    sqlContext.udf.register("remove_random_prefix", (rem: String) => {
      rem.split("_")(0)
    })

//    sqlContext.udf.register("group_concat_distinct", (elem: String) => {
//      elem.groupBy
//    })
    sqlContext.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF)
    MockData.mock(sc, sqlContext)
    val taskDAO: ITaskDAO = DAOFactory.getTaskDAO()
    val taskid: Long = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_TOPN_MONITOR_FLOW)
    val task: Task = taskDAO.findTaskById(taskid)
    if (task == null) {
      System.exit(-1)
    }
    System.out.println("task.getTaskParams()----" + task.taskParams)
    val taskParam: JSONObject = JSON.parseObject(task.taskParams)
    val areaId2DetailInfos: RDD[(String, Row)] = getInfosByDateRDD(sqlContext, taskParam)
    println("----------------areaId2DetailInfos---------------")
    areaId2DetailInfos.foreach(println(_))
    val areaId2AreaInfoRDD: RDD[(String, String)] = getAreaId2AreaInfoRDD(sqlContext)

    /**
      * 补全区域信息    添加区域名称
      * monitor_id car road_id	area_id	area_name
      * 生成基础临时信息表
      * tmp_car_flow_basic
      *
      * 将符合条件的所有的数据得到对应的中文区域名称 ，然后动态创建Schema的方式，将这些数据注册成临时表tmp_car_flow_basic
      */
   generateTempRoadFlowBasicTable(sqlContext, areaId2DetailInfos, areaId2AreaInfoRDD)

    /**
      * 统计各个区域各个路段车流量的临时表
      *
      * area_name  road_id    car_count      monitor_infos
      * 海淀区		  01		 100	  0001=20|0002=30|0003=50
      *
      * 注册成临时表tmp_area_road_flow_count
      */
   generateTempAreaRoadFlowTable(sqlContext)
   getAreaTop3RoadFolwRDD(sqlContext)

  }


  def getAreaTop3RoadFolwRDD(sqlContext:SQLContext){
    /**
      * tmp_area_road_flow_count表：
      *      area_name
      *      road_id
      *      car_count
      *      monitor_infos
      */
   val sql =
      "SELECT "+
        "area_name,"+
        "road_id,"+
        "car_count,"+
        "monitor_infos, "+
        "CASE "+
        "WHEN car_count > 170 THEN 'A LEVEL' "  +
        "WHEN car_count > 160 AND car_count <= 170 THEN 'B LEVEL' " +
        "WHEN car_count > 150 AND car_count <= 160 THEN 'C LEVEL' " +
        "ELSE 'D LEVEL' " +
        "END flow_level " +
        "FROM (" +
        "SELECT " +
        "area_name,"  +
        "road_id,"  +
        "car_count," +
        "monitor_infos,"  +
        "row_number() OVER (PARTITION BY area_name ORDER BY car_count DESC) rn "+
        "FROM tmp_area_road_flow_count " +
        ") tmp " +
        "WHERE rn <=3"
    val df:DataFrame = sqlContext.sql(sql)
    System.out.println("--------最终的结果-------")
    df.show()
  }



  def generateTempAreaRoadFlowTable(sqlContext: SQLContext): Unit = {
    val sql: String = "SELECT " + "area_name," + "road_id," + "count(*) car_count," +
      "group_concat_distinct(monitor_id) monitor_infos " +
      "FROM tmp_car_flow_basic " +
      "GROUP BY area_name,road_id"
    val sqlText: String = "SELECT " + "area_name_road_id," + "sum(car_count)," +
      "group_concat_distinct(monitor_infos) monitor_infoss " +
      "FROM (" + "SELECT " +
      "remove_random_prefix(prefix_area_name_road_id) area_name_road_id," +
      "car_count," + "monitor_infos " + "FROM (" +
      "SELECT " + "prefix_area_name_road_id," +
      "count(*) car_count," + "group_concat_distinct(monitor_id) monitor_infos " +
      "FROM (" + "SELECT " + "monitor_id," +
      "car," +
      "random_prefix(concat_String_string(area_name,road_id,':'),10) prefix_area_name_road_id " +
      "FROM tmp_car_flow_basic " +
      ") t1 " +
      "GROUP BY prefix_area_name_road_id " +
      ") t2 " +
      ") t3 " +
      "GROUP BY area_name_road_id"

    val df: DataFrame = sqlContext.sql(sql)
    println("*******************************")
    df.show()
    df.registerTempTable("tmp_area_road_flow_count")

  }

  /**
    * 获取符合条件数据对应的区域名称，并将这些信息注册成临时表 tmp_car_flow_basic
    *
    * @param sqlContext
    * @param areaId2DetailInfos
    * @param areaId2AreaInfoRDD
    */
  def generateTempRoadFlowBasicTable(sqlContext: SQLContext,
                                     areaId2DetailInfos: RDD[(String, Row)], areaId2AreaInfoRDD: RDD[(String, String)]) = {
 //   println("111111111111111111111111111111111111111111111111")
    val tmp =areaId2DetailInfos.join(areaId2AreaInfoRDD).foreach(println(_))
   val tmpRowRDD: RDD[Row] = areaId2DetailInfos.join(areaId2AreaInfoRDD).map(x => {
     RowFactory.create(x._1.toString, x._2._2.toString, x._2._1(2).toString, x._2._1(0).toString, x._2._1(1).toString)
   })
    import org.apache.spark.sql.types.DataTypes
    val structFields: Array[StructField] = Array[StructField](
      (DataTypes.createStructField("area_name", DataTypes.StringType, true)) ,
        (DataTypes.createStructField("road_id", DataTypes.StringType, true)),
      (DataTypes.createStructField("monitor_id", DataTypes.StringType, true)),
      (DataTypes.createStructField("car", DataTypes.StringType, true)))
    val schema: StructType = DataTypes.createStructType(structFields)
    val df: DataFrame = sqlContext.createDataFrame(tmpRowRDD, schema)


    df.registerTempTable("tmp_car_flow_basic")
  }

  def getAreaId2AreaInfoRDD(sqlContext: SQLContext) = {
    var url: String = null
    var user: String = null
    var password: String = null
    var local: Boolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    println("-------------------------------------")
    //获取Mysql数据库的url,user,password信息
    if (local) {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL)
      user = ConfigurationManager.getProperty(Constants.JDBC_USER)
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
    }
    else {
      url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD)
      user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD)
      password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD)
    }

    val options: Map[String, String] = Map[String, String]("url" -> url,
      "user" -> user,
      "password" -> password,
      "dbtable" -> "area_info"
    )
    val areaInfoDF: DataFrame = sqlContext.read.format("jdbc").options(options).load()
    System.out.println("------------Mysql数据库中的表area_info数据为------------")
    areaInfoDF.show()
    val areaInfoRDD: RDD[Row] = areaInfoDF.rdd
    val areaid2areaInfoRDD: RDD[(String, String)] = areaInfoRDD.map(x => {
      (x(0).toString, x(1).toString)
    })
    areaid2areaInfoRDD
  }

  /**
    * 获取日期内的数据
    *
    * @param sqlContext
    * @param taskParam
    * @return (areaId,row)
    */
  def getInfosByDateRDD(sqlContext: SQLContext, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "SELECT " + "monitor_id," + "car," + "road_id," + "area_id " + "FROM	monitor_flow_action " + "WHERE date >= '" + startDate + "'" + "AND date <= '" + endDate + "'"
    val df: DataFrame = sqlContext.sql(sql)
    df.rdd.map(row => {
      val areaId = row.getString(3)
      (areaId, row)
    })
  }

}
