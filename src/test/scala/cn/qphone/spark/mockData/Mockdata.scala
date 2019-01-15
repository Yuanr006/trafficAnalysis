package cn.qphone.spark.mockData


import cn.qphone.spark.util.{DateUtils, StringUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/14 20:27
  * @Description: 模拟生成离线数据
  */
object Mockdata {
  /**
    * 模拟数据 数据格式如下
    * 日期  卡口ID        摄像头编号  车牌号  拍摄时间      车速   道路ID     区域ID
    * date	 monitor_id	 camera_id	  car	    action_time		speed	 road_id		area_id
    * 表名
    * monitor_flow_action
    * monitor_camera_info
    *
    * @param sc
    * @param sqlContext
    */
  def mock(sc:SparkContext,sqlContext:SQLContext): Unit ={
    val dataList: ListBuffer[Row]= new ListBuffer[Row]
    val random: Random=new Random
    val locations: Array[String]=Array("鲁","京","京","京","沪","京","京","深","京","京")
    val nowDate=DateUtils.getTodayDate


    /**
      * 模拟3000车辆
      */
    for (i<-1 to 3000){
      //模拟车牌号：如：京88888
      val carNum: String=locations(random.nextInt(5))+(65+random.nextInt(26)).toChar+StringUtils.fulfuill(5,random.nextInt(100000).toString)
      //baseActionTime 模拟24小时
      //2019-01-01 01
      var baseActionTime=nowDate+" "+StringUtils.fulfuill(random.nextInt(24).toString);

      /**
        * 焖鸡不同卡扣的不同摄像头的数据
        */
      for(j<-0 to random.nextInt(300)){
        //模拟每个车辆每被30个摄像头拍摄后 时间上累计加1小时。这样做使数据更加真实。
        if (j%30==0&&j!=0){
          baseActionTime=nowDate+" "+ StringUtils.fulfuill((baseActionTime.split(" ")(1).trim.toInt+1).toString)
        }
        //模拟经过此卡扣开始时间 ，如：2018-01-01 20:09:10
        val actionTime: String=baseActionTime+":"+ StringUtils.fulfuill(random.nextInt(60).toString) + ":"+StringUtils.fulfuill(random.nextInt(60).toString);
        //模拟9个卡扣monitorId，0补全4位
        val monitorId: String=StringUtils.fulfuill(4,random.nextInt(9).toString)
        //模拟速度
        val speed: String=(random.nextInt(260)+1).toString
        //模拟道路id 【1~50 个道路】
        val roadId: String=(random.nextInt(50)+1).toString
        //模拟摄像头id cameraId
        val cameraId: String=StringUtils.fulfuill(5,random.nextInt(100000).toString)
        //模拟areaId 【一共8个区域】
        val areaId=StringUtils.fulfuill((2,random.nextInt(8)+1).toString)
        dataList+=Row(nowDate,monitorId,cameraId,carNum,actionTime,speed,roadId,areaId)

      }
      val rdd:RDD[Row]=sc.makeRDD(dataList)
      val struceType:StructType=StructType(Seq(
        StructField("date", StringType, true),
          StructField("monitor_id", StringType, true),
          StructField("camera_id", StringType, true),
          StructField("car", StringType, true),
          StructField("action_time", StringType, true),
          StructField("speed", StringType, true),
          StructField("road_id", StringType, true),
          StructField("area_id", StringType, true)
      ))
      val df: DataFrame=sqlContext.createDataFrame(rdd,struceType)
      df.createOrReplaceTempView("monitor_flow_action")
      //默认打印出来df里面的20行数据
      System.out.println("----打印 车辆信息数据----");
      df.show(5)


    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf=new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("mockData")
    val sc:SparkContext=new SparkContext(sparkConf)
    val sqlContext:SQLContext=new SQLContext(sc)
    mock(sc,sqlContext)
  }
}
