package cn.qphone.spark.rtmroad

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.api.java.function.Function
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/14 22:23
  * @Description: ${Description}
  */
object RoadRealTimeAnalyze  {
  def main(args: Array[String]): Unit = {
    val secondFormate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    //构建Spark Streaming
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("AdClickRealTimeStatSpark")

    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("./checkpoint")
    var brokerList ="bigdata1:9092"
    var kafkaParams = Map[String,Object](
      "bootstrap.servers" -> brokerList,//Kafka服务监听端口
      "key.deserializer" -> classOf[StringDeserializer],//指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
      "value.deserializer" -> classOf[StringDeserializer],//指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
      "group.id" -> "g100",//消费者ID，随意指定
      "auto.offset.reset" -> "latest",//指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
      "enable.auto.commit" -> (false:java.lang.Boolean)//如果true,consumer定期地往zookeeper写入每个分区的offset
    )
    val topics = Array("RoadRealTimeLog")

    var directStream: InputDStream[ConsumerRecord[String,String]]= KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics,kafkaParams)
    )
    val result = directStream.map(_.value())
      .flatMap(_.split(System.lineSeparator()))

    val speedCount = result.map(x=>(x.split("\t")(1),x.split("\t")(5).toInt))
      .updateStateByKey(updateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),true).map(x=>println(x._2))

    val monitor = result.map(x=>(x.split("\t")(1),1)).updateStateByKey(updateFunc,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),
      true).map(x=>println(
        "当前时间：" + secondFormate.format(Calendar.getInstance.getTime)+
        "卡扣编号：" + x._1 +
        "车辆总数：" + x._2 +
        "速度总数：" + speedCount +
        "平均速度：" + (speedCount / x._2)))

    directStream.foreachRDD(rdd=>{
      //offset：用来保存消费进度,因为Kafka是一个数据队列所以这个偏移量就是记录消费到哪来了
      //将rdd强转为HasOffsetRanges得Array[OffsetRange]
      val offsetRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val maped:RDD[(String, String)] = rdd.map(record=>(record.key(),record.value()))
      //计算逻辑
      maped.foreach(println)
      //循环输出(该偏移量范围内的主题，分区id，该偏移量范围的起点偏移量，该偏移量范围的终点偏移量)
      for (o <- offsetRange){
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    })
    //开启了很多调度器，接收监控器...
    ssc.start()
    //等待结束
    ssc.awaitTermination()
  }

  /**
    * "当前时间：" + secondFormate.format(Calendar.getInstance.getTime) +
    * "卡扣编号：" + monitor +
    * "车辆总数：" + carCount +
    * "速度总数：" + speedCount +
    * " 平均速度：" + (speedCount / carCount);
    * @param adRealTimeLogDStream
    */
  /**
    * Seq[Int] 当前wordCount业务中实时产生的value值Seq(1,1,1,...;)
    * Option[Int] 从统计开始到这个批次数据为止的历史值
    */
  val updateFunc = (it : Iterator[(String, Seq[Int], Option[Int])]) => {
    //这行代码与以下代码等价
    it.map{case (w,s,o) => (w,s.sum + o.getOrElse(0))}
    //      it.map(x=>{
    //        (x._1,x._3.getOrElse(0)+x._2.sum)
    //      })
  }
}
