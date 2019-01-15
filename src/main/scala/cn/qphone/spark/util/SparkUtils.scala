package cn.qphone.spark.util



import cn.qphone.spark.conf.ConfigurationManager
import cn.qphone.spark.constant.Constants
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext

/**
  * Spark工具类
  *
  */
object SparkUtils extends Constants {
  /**
    * 根据当前是否本地测试的配置，决定 如何设置SparkConf的master
    */
  def setMaster(conf: SparkConf): Unit = {
    val local = ConfigurationManager.getBoolean(SPARK_LOCAL)
    if (local) conf.setMaster("local")
  }

  /**
    * 获取SQLContext
    * 如果spark.local设置为true，那么就创建SQLContext；否则，创建HiveContext
    *
    * @param sc
    * @return
    */


  def getSQLContext(sc: JavaSparkContext): SQLContext = {
    val local = ConfigurationManager.getBoolean(SPARK_LOCAL)
    if (local) {
      new SQLContext(sc)
    }
    else {
      new Nothing(sc)
    }
  }


  /**
    * 生成模拟数据
    * 如果spark.local配置设置为true，则生成模拟数据；否则不生成
    *
    * @param sc
    * @param sqlContext
    */
  def mockData(sc: JavaSparkContext, sqlContext: SQLContext): Unit = {
    val local = ConfigurationManager.getBoolean(SPARK_LOCAL)

    /**
      * 如何local为true  说明在本地测试  应该生产模拟数据    RDD-》DataFrame-->注册成临时表0
      * false    HiveContext  直接可以操作hive表
      */
    //    if (local) {
    //      MockData.mock(sc, sqlContext)
    //    }
  }
}

