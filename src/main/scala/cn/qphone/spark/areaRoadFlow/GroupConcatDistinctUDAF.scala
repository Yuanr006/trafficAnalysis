package cn.qphone.spark.areaRoadFlow

import cn.qphone.spark.util.StringUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}


/**
  *  组内拼接去重函数（group_concat_distinct()）
  *  技术点：自定义UDAF聚合函数
  **/

class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{
  // 指定输入数据的字段与类型
  override def inputSchema: StructType = StructType(Array(StructField("carInfo",StringType,true)))
  // 指定缓冲数据的字段与类型
  override def bufferSchema: StructType = StructType(Array(StructField("bufferInfo",StringType,true)))
  // 指定返回类型
  override def dataType: DataType = StringType
  // 指定是否是确定性的
  override def deterministic: Boolean = true
  /**
    * 初始化
    * 可以认为是，你自己在内部指定一个初始的值
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, "")

  /**
    * 更新
    * 可以认为是，一个一个地将组内的字段值传递进来
    * 实现拼接的逻辑
    */
  override def update(buffer: MutableAggregationBuffer, input: Row) = {
    var bufferMonitorInfo:String = buffer.getString(0)
    val inputMonitorInfo:String = input.getString(0)
    val split:Array[String] = inputMonitorInfo.split("\\|")
    var monitorId = ""
    var addNum = 1
    for (string<-split){
      if (string.indexOf("=") != -1){
        monitorId = string.split("=")(0)
        addNum = string.split("=")(1).toInt
      }else{
        monitorId = string
      }
      val oldVS = StringUtils.getFieldFromConcatString(bufferMonitorInfo, "\\|",monitorId)
      if (oldVS == null || oldVS == ""){
        bufferMonitorInfo += "|"+monitorId+"="+addNum
      }else{
        bufferMonitorInfo = StringUtils.setFieldInConcatString(bufferMonitorInfo, "\\|", monitorId,(oldVS.toInt+addNum).toString())
      }
      buffer.update(0,bufferMonitorInfo)
    }
  }



  /**
    * 合并
    * update操作，可能是针对一个分组内的部分数据，在某个节点上发生的
    * 但是可能一个分组内的数据，会分布在多个节点上处理
    * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
    * merge1:|0001=100|0002=20|0003=4
    * merge2:|0001=200|0002=30|0003=3
    *
    * 海淀区 建材城西路1
    * 海淀区 建材城西路2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferMonitorInfo1:String = buffer1.getString(0)
    var bufferMonitorInfo2:String = buffer2.getString(0)
    for ( monitorInfo <- bufferMonitorInfo2.split("\\|")){
      val map:Map[String, String] = StringUtils.getKeyValuesFromConcatString(monitorInfo, "\\|")
      for (entry <- map){
        val monitor:String = entry._1
        val carCount:Int = entry._2.toInt
        val oldVS = StringUtils.getFieldFromConcatString(bufferMonitorInfo1, "\\|", monitor)
        if (oldVS == null){
          if ("".equals(bufferMonitorInfo1)){
            bufferMonitorInfo1 += monitor + "=" + carCount
          }else{
            bufferMonitorInfo1 += "|" + monitor + "=" + carCount
          }
        }else{
          var oldVal:Int = oldVS.toInt
          oldVal += carCount
          bufferMonitorInfo1 = StringUtils.setFieldInConcatString(bufferMonitorInfo1, "\\|", monitor, oldVal+"")

        }
        buffer1.update(0, bufferMonitorInfo1)
      }
    }
  }
  /**
    * evaluate方法返回数据的类型要和dateType的类型一致，不一致就会报错
    */
  override def evaluate(buffer: Row): Any = buffer.getString(0)

}
