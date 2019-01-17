package cn.qphone.spark.areaRoadFlow

import cn.qphone.spark.bean.Task
import cn.qphone.spark.conf.ConfigurationManager
import cn.qphone.spark.constant.Constants
import cn.qphone.spark.dao.ITaskDAO
import cn.qphone.spark.dao.factory.DAOFactory
import cn.qphone.spark.mockData.MockData
import cn.qphone.spark.util.{ParamUtils, StringUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 计算出每一个区域top3的道路流量
  * 每一个区域车流量最多的3条道路   每条道路有多个卡扣
  * 这是一个分组取topN  SparkSQL分组取topN
  * 区域，道路流量排序         按照区域和道路进行分组
  *
  */
object AreaTop3RoadFlowAnalyze {
  def main(args: Array[String]): Unit = {
    //配置Spark环境
    val conf: SparkConf = new SparkConf().setAppName("AreaTop3ProductSpark").set("spark.sql.shuffle.partitions", "1").setMaster("local[*]")
    System.setProperty("HADOOP_USER_NAME", "root")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //注册ConcatStringStringUDF为UDF函数,参数为area_name: String, road_id: String, split: String，输出值为拼接字符串
    sqlContext.udf.register("ConcatStringStringUDF", (area_name: String, road_id: String, split: String) => {
      area_name + split + road_id
    })
    //注册random_prefix为UDF函数,参数为area_name_road_id: String, ranNum: Integer，输出值为为参数加上一个随机数
    sqlContext.udf.register("random_prefix", (area_name_road_id: String, ranNum: Integer) => {
      (new Random).nextInt(ranNum) + "_" + area_name_road_id
    })
    //注册remove_random_prefix为UDF函数,参数为rem: String，输出值为去除为参数加上的随机数
    sqlContext.udf.register("remove_random_prefix", (rem: String) => {
      rem.split("_")(1)
    })
    //注册group_concat_distinct为UDAF函数，输出值为为分组去重并拼接
    sqlContext.udf.register("group_concat_distinct", new GroupConcatDistinctUDAF)
    //模拟制造数据
    MockData.mock(sc, sqlContext)
    //通过DAO工厂实例化TaskDAO对象
    val taskDAO: ITaskDAO = DAOFactory.getTaskDAO()
    //通过调用ParamUtils.getTaskIdFromArgs获取到taskid
    val taskid: Long = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_TOPN_MONITOR_FLOW)
    //在实例化的taskDAO中查询调用ParamUtils.getTaskIdFromArgs获取到taskid
    val task: Task = taskDAO.findTaskById(taskid)
    //如果在实例化的taskDAO中没有查询到调用ParamUtils.getTaskIdFromArgs获取到taskid，那么程序退出
    if (task == null) {
      System.exit(-1)
    }
    System.out.println("task.getTaskParams()----" + task.taskParams)
    //将task.taskParams解析成JSONObject对象
    val taskParam: JSONObject = JSON.parseObject(task.taskParams)
    //通过getInfosByDateRDD方法获取到RDD[(String, Row)]对象
    //（area_id，（monitor_id,car,road_id,area_id））
    val areaId2DetailInfos: RDD[(String, Row)] = getInfosByDateRDD(sqlContext, taskParam)
    //    println("----------------areaId2DetailInfos---------------")
    //    areaId2DetailInfos.foreach(println(_))
    //通过getInfosByDateRDD方法获取到RDD[(String, Row)]对象
    //（ areaid，areaInfo）
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

    /**
      * 统计各个区域车流量的临时表
      * 并落地
      */
    getAreaTop3RoadFolwRDD(sqlContext)

  }


  def getAreaTop3RoadFolwRDD(sqlContext: SQLContext) {
    /**
      * tmp_area_road_flow_count表：
      * area_name
      * road_id
      * car_count
      * monitor_infos
      */
    val sql =
      "SELECT " +
        "area_name," +
        "road_id," +
        "car_count," +
        "monitor_infos, " +
        "CASE " +
        "WHEN car_count > 170 THEN 'A LEVEL' " +
        "WHEN car_count > 160 AND car_count <= 170 THEN 'B LEVEL' " +
        "WHEN car_count > 150 AND car_count <= 160 THEN 'C LEVEL' " +
        "ELSE 'D LEVEL' " +
        "END flow_level " +
        "FROM (" +
        "SELECT " +
        "area_name," +
        "road_id," +
        "car_count," +
        "monitor_infos," +
        "row_number() OVER (PARTITION BY area_name ORDER BY car_count DESC) rn " +
        "FROM tmp_area_road_flow_count " +
        ") tmp " +
        "WHERE rn <=3"
    val df: DataFrame = sqlContext.sql(sql)
    df.toJavaRDD.saveAsTextFile("c:/a")
    System.out.println("--------最终的结果-------")
    df.show()
    //存入Hive中，要有result 这个database库
    sqlContext.sql("use result")
    sqlContext.sql("drop table if exists result.areaTop3Road")
    df.write.saveAsTable("areaTop3Road")

  }

  //tmp_car_flow_basic表： (area_id,AreaInfo,road_id,monitor_id,car)
  def generateTempAreaRoadFlowTable(sqlContext: SQLContext): Unit = {
    //使用sql语句查询tmp_car_flow_basic表中根据area_name,road_id分组后的 （area_name,road_id,car_count，monitor_infos）
    //      "group_concat_distinct(monitor_id) monitor_infos "
    val sqlTmp = "SELECT area_name, road_id, sum(car_count) car_count, collect_Set(monitor_infos) monitor_infos FROM ( SELECT area_name, road_id, car_count, concat_ws( '=', monitor_id, monitor_infos ) monitor_infos FROM ( SELECT area_name, road_id, count( * ) car_count, monitor_id, count( monitor_id ) monitor_infos  FROM tmp_car_flow_basic  GROUP BY area_name, road_id, monitor_id  ) AS everymonitor_monitor_infos  )everyRoad_monitor_infos GROUP BY everyRoad_monitor_infos.area_name,everyRoad_monitor_infos.road_id "
    val df = sqlContext.sql(sqlTmp)



//    val sql: String = "SELECT " + "area_name," + "road_id," + "count(*) car_count," +
//      "group_concat_distinct(monitor_id) monitor_infos " +
//      "FROM tmp_car_flow_basic " +
//      "GROUP BY area_name,road_id"
    //如果存在数据倾斜，则用这个sql
    //    val sqlText: String = "SELECT " + "area_name_road_id," + "sum(car_count)," +
    //      "group_concat_distinct(monitor_infos) monitor_infoss " +
    //      "FROM (" + "SELECT " +
    //      "remove_random_prefix(prefix_area_name_road_id) area_name_road_id," +
    //      "car_count," + "monitor_infos " + "FROM (" +
    //      "SELECT " + "prefix_area_name_road_id," +
    //      "count(*) car_count," + "group_concat_distinct(monitor_id) monitor_infos " +
    //      "FROM (" + "SELECT " + "monitor_id," +
    //      "car," +
    //      "random_prefix(concat_String_string(area_name,road_id,':'),10) prefix_area_name_road_id " +
    //      "FROM tmp_car_flow_basic " +
    //      ") t1 " +
    //      "GROUP BY prefix_area_name_road_id " +
    //      ") t2 " +
    //      ") t3 " +
    //      "GROUP BY area_name_road_id"
    //构造DataFrame
  //  val df: DataFrame = sqlContext.sql(sql)
  //  println("*******************************")
    df.show()
    //将获取的信息注册成临时表 tmp_area_road_flow_count
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
    //   println("-----------------------------------------")
    //将（area_id，（monitor_id,car,road_id,area_id））与（areaId，AreaInfo）join操作
    // val tmp = areaId2DetailInfos.join(areaId2AreaInfoRDD).foreach(println(_))
    //将（area_id，（monitor_id,car,road_id,area_id））与（areaId，AreaInfo）join操作后每一个RDD通过RowFactory创建一个
    //(area_id,AreaInfo,road_id,monitor_id,car)
    val tmpRowRDD: RDD[Row] = areaId2DetailInfos.join(areaId2AreaInfoRDD).map(x => {
      RowFactory.create(x._1.toString, x._2._2.toString, x._2._1(2).toString, x._2._1(0).toString, x._2._1(1).toString)
    })
    import org.apache.spark.sql.types.DataTypes
    //动态构建DataFrame的元数据
    val structFields: Array[StructField] = Array[StructField](
      (DataTypes.createStructField("area_id", DataTypes.StringType, true)),
      (DataTypes.createStructField("area_name", DataTypes.StringType, true)),
      (DataTypes.createStructField("road_id", DataTypes.StringType, true)),
      (DataTypes.createStructField("monitor_id", DataTypes.StringType, true)),
      (DataTypes.createStructField("car", DataTypes.StringType, true)))
    //构建StructType用于DataFrame 元数据的描述
    val schema: StructType = DataTypes.createStructType(structFields)
    //基于MeataData以及RDD<Row>来构造DataFrame
    val df: DataFrame = sqlContext.createDataFrame(tmpRowRDD, schema)

    //将获取的信息注册成临时表 tmp_car_flow_basic
    df.registerTempTable("tmp_car_flow_basic")
  }

  /**
    * 通过AreaId获取到AreaInfo并返回RDD元组（RDD（））
    *
    * @param sqlContext
    * @return
    */
  def getAreaId2AreaInfoRDD(sqlContext: SQLContext) = {
    var url: String = null
    var user: String = null
    var password: String = null
    //获取SPARK程序是否为LOCAL模式
    var local: Boolean = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    // println("-------------------------------------")
    //获取Mysql数据库的url,user,password信息
    //如果SPARK程序为LOCAL模式
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
    //将获取到的url，user，password以他们对应的属性为key转化成map映射
    val options: Map[String, String] = Map[String, String]("url" -> url,
      "user" -> user,
      "password" -> password,
      "dbtable" -> "area_info"
    )
    //sparksql连接jdbc获取到对应表数据
    val areaInfoDF: DataFrame = sqlContext.read.format("jdbc").options(options).load()
    System.out.println("------------Mysql数据库中的表area_info数据为------------")
    areaInfoDF.show()
    //将获取的表数据areaInfoDF转化成RDD
    val areaInfoRDD: RDD[Row] = areaInfoDF.rdd
    //将获取的表数据areaInfoDF转化成的RDD进行map操作，将信息转化成Tuple2（ areaid，areaInfo）
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
    //通过 ParamUtils.getParam获取到开始时间
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    //通过 ParamUtils.getParam获取到结束时间
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    //使用sql语句查询monitor_flow_action表中startDate&endDate时间内的 monitor_id,car,road_id,area_id
    val sql = "SELECT " + "monitor_id," + "car," + "road_id," + "area_id " + "FROM	monitor_flow_action " + "WHERE date >= '" + startDate + "'" + "AND date <= '" + endDate + "'"
    //将sql得到的结果转化成DataFrame
    val df: DataFrame = sqlContext.sql(sql)
    //将sql得到的结果转化成的DataFrame再次转换成rdd，并对其中的每一个元素过去到第三列组成一个Tuple元组（area_id，（monitor_id,car,road_id,area_id））
    df.rdd.map(row => {
      val areaId = row.getString(3)
      (areaId, row)
    })
  }

}

