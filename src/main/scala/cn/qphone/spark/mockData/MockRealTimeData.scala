package cn.qphone.spark.mockData

import java.util.Properties

import cn.qphone.spark.util.{DateUtils, StringUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

import scala.util.Random

object MockRealTimeData extends Thread{
  final val random = new Random
  final val locations = Array[String]("鲁", "京", "京", "京", "沪", "京", "京", "深", "京", "京")
  val prop = new Properties()
  def main(args: Array[String]): Unit = {
    //使用java.util.Properties类的对象来封装一些连接kafka必备的配置属性
    val kafkaProducerProperties = new Properties
    //指定broker的地址清单
    kafkaProducerProperties.put("bootstrap.servers", prop.getProperty("kafka.metadata.broker.list"))
    //必须设置，就算打算只发送值内容
    kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //指定的类会将值序列化
    kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //指定多少分区副本收到消息，生产者才会任务消息写入成功
    kafkaProducerProperties.put("acks", "all")
    var producer:Producer[String,String]=null
    try {
      producer = new KafkaProducer(kafkaProducerProperties)
      while (true) {
        val date = DateUtils.getTodayDate
        var baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(23) + "")
        baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")(1)) + 1) + "")
        //拍摄时间
        val actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60) + "") + ":" + StringUtils.fulfuill(random.nextInt(60) + "")
        //卡口id
        val monitorId = StringUtils.fulfuill(4, random.nextInt(9) + "")
        //车牌号码
        val car = locations(random.nextInt(10)) + (65 + random.nextInt(26)).asInstanceOf[Char] + StringUtils.fulfuill(5, random.nextInt(99999) + "")
        //车速
        val speed = random.nextInt(260) + ""
        //道路id
        val roadId = random.nextInt(50) + 1 + ""
        //摄像头编号
        val cameraId = StringUtils.fulfuill(5, random.nextInt(9999) + "")
        //区域id
        val areaId = StringUtils.fulfuill(2, random.nextInt(8) + "")
        producer.send(new ProducerRecord("test", date + "\t" + monitorId + "\t" + cameraId + "\t" + car + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId))
        println(date + "\t" + monitorId + "\t" + cameraId + "\t" + car + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId)
        //休眠500ms
        Thread.sleep(500)
      }
    } catch {
      case e: Exception => e.printStackTrace
    }finally {
      //关闭资源
      producer.close()
    }
  }
}



