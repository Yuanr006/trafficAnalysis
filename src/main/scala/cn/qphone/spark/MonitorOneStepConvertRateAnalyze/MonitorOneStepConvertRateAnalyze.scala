package cn.qphone.spark.MonitorOneStepConvertRateAnalyze

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

  }
}
