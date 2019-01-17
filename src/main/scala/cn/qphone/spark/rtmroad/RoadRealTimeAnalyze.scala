package cn.qphone.spark.rtmroad

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/14 22:23
  * @Description: ${Description}
  */
object RoadRealTimeAnalyze {
  val prop = new Properties()
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("AdClickRealTimeStatSpark")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置checkpoint
    ssc.checkpoint("D:\\study\\kafka\\checkpoint")
    //设置警告级别
    ssc.sparkContext.setLogLevel("WARN")
    //设置kafka消费者属性
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> prop.getProperty("kafka.metadata.broker.list"), //Kafka服务监听端口
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test", //消费者id
      "auto.offset.reset" -> "latest",//指定从latest处开始读取数据
      "enable.auto.commit" -> (false: java.lang.Boolean)//如果true,consumer定期地往zookeeper写入每个分区的offset
    )
    //设置监控的主题，可以同时监听多个
    val topics = Array("test")
    //指定kafka数据源
    var stream= KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )
    realTimeCalculateRoadState(stream)
    //    开启了很多调度器，接收监控器...
    ssc.start()
    //等待结束
    ssc.awaitTermination()
  }
  def realTimeCalculateRoadState(stream: InputDStream[ConsumerRecord[String,String]]): Unit ={
    val roadRealTimeLog = stream.map(_.value())
    //拿到车辆的信息了
    val result = roadRealTimeLog.map(line => {
      val lineArr = line.split("\t")
      //<monitorId	<speed,1>>
      (lineArr(1), (lineArr(5).toInt, 1))
    }).reduceByKeyAndWindow((x,y) =>{
      (x._1+y._1, x._2+y._2)
    }, (x,y)=>{
      (x._1-y._1, x._2-y._2)
    },Seconds(5), Seconds(5))

    //获取时间格式化器
    val formate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    result.foreachRDD(rdd=>{
      def func(rdd:Iterator[(String, (Int, Int))])={
        var conn : Connection = null
        var stmt : PreparedStatement = null
        try{
          val url = prop.getProperty("jdbc.url")
          val user = prop.getProperty("jdbc.user")
          val password = prop.getProperty("jdbc.password")
          conn = DriverManager.getConnection(url, user, password)
          val date = new Date()
          //获取当前时间
          var now = formate.format(date)
          rdd.foreach(x=>{
            //卡扣编号
            val monitor = x._1
            //速度总数
            var speedCount = x._2._1
            //车辆总数
            var cars = x._2._2
            try{
              //有车通过时
              println("当前时间：" + now +
                "卡扣编号：" + monitor +
                "车辆总数：" + cars +
                "速度总数：" + speedCount +
                "平均速度：" + (speedCount / cars))
            }catch {
              //没有车通过时
              case e:ArithmeticException=>println("当前时间："+now+ "卡扣编号："+monitor + " 没有车通过")
            }
            println("====================================================================================")
            val sql = "insert into roadrealtimeanalyze values(?,?,?,?,?)"
            stmt = conn.prepareStatement(sql)
            stmt.setString(1, now)
            stmt.setString(2, monitor)
            stmt.setString(3, cars.toString)
            stmt.setString(4, speedCount.toString)
            stmt.setString(5, (speedCount / cars).toString)
            stmt.executeUpdate()
          })
        }catch {
          case e: Exception => print()
        }
      }
      val repartitionedRDD = rdd.repartition(5)
      repartitionedRDD.foreachPartition(func)
    })
  }
}

