package cn.qphone.spark.mockData

import com.sun.rowset.internal.Row
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
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
  //  val nowDate=DateUtil.getTodayDate

    /**
      * 模拟3000车辆
      */
    for (i<-1 to 3000){
      //模拟车牌号：如：京88888
      val carNum: String=locations(random.nextInt(5))
    }
  }
}
