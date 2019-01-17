package cn.qphone.spark.areaRoadFlow

import cn.qphone.spark.util.StringUtils
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}


/**
  *  组内拼接去重函数（group_concat_distinct()）
  *  技术点：自定义UDAF聚合函数
  **/
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{
  // 指定输入数据的字段与类型
  override def inputSchema: StructType = StructType(List(StructField("carInfo",StringType,true)))
  // 指定缓冲数据的字段与类型
  override def bufferSchema: StructType = StructType(List(StructField("bufferInfo",StringType,true)))
  // 指定返回类型
  override def dataType: DataType = StringType
  // 指定是否是确定性的
  override def deterministic: Boolean = true
  /**
    * 初始化缓冲区域的值
    * 可以认为是，你自己在内部指定一个初始的值
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, "")

  /**
    * 更新
    * 可以认为是，一个一个地将组内的字段值传递进来
    * 实现拼接的逻辑
    */
  override def update(buffer: MutableAggregationBuffer, input: Row) = {
    // 缓冲中的已经拼接过的monitor信息小字符串bufferMonitorInfo
    var bufferMonitorInfo:String = buffer.getString(0)
    // 每一次传进来的车辆信息inputMonitorInfo
    val inputMonitorInfo:String = input.getString(0)
    // 把每一次传进来的车辆信息inputMonitorInfo里面的数据都拆开来更新
    val split:Array[String] = inputMonitorInfo.split("\\|")
    var monitorId = ""
    var addNum = 1
    //遍历每一次传进来的车辆信息inputMonitorInfo里面拆开的数据
    for (string:String<-split){
      //如果遍历得到的每一次传进来的车辆信息inputMonitorInfo里面拆开得到的数据对应字段都是有值的
      if (string.indexOf("=") != -1){
        //将遍历得到的每一次传进来的车辆信息inputMonitorInfo里面拆开得到的数据对应字段赋值给monitorId，value赋值给addNum
        monitorId = string.split("=")(0)
        addNum = string.split("=")(1).toInt
      }else{
        //如果遍历得到的每一次传进来的车辆信息inputMonitorInfo里面拆开得到的数据对应字段是没有值的，
        // 说明该字段是没有值的，那么直接将穿进来的车辆信息拆开得到的数据直接赋值给monitorId
        monitorId = string
      }
      // 将缓冲中的已经拼接过的monitor信息小字符串bufferMonitorInfo中提取字段值赋值给oldVS
      val oldVS:String = StringUtils.getFieldFromConcatString(bufferMonitorInfo, "\\|",monitorId)
      //如果缓冲中的已经拼接过的monitor信息小字符串bufferMonitorInfo中提取字段值oldVS为空
      if (oldVS == null || oldVS == ""){
        //那么直接将传进来的车辆信息拆开得到的数据monitorId，addNum加上分隔符直接拼接进bufferMonitorInfo
        bufferMonitorInfo += "|"+monitorId+"="+addNum
      }else{
        //如果缓冲中的已经拼接过的monitor信息小字符串bufferMonitorInfo中提取字段值oldVS不为空
        //调用 StringUtils.setFieldInConcatString方法，将
        //如果缓冲中的已经拼接过的monitor信息小字符串bufferMonitorInfo中提取字段值oldVS 加上
        //传进来的车辆信息拆开得到的数据addNum
        //用竖划线分割拼接在bufferMonitorInfo后面
        bufferMonitorInfo = StringUtils.setFieldInConcatString(bufferMonitorInfo, "\\|", monitorId,(oldVS.toInt+addNum).toString)
      }
      //将拼接得到的bufferMonitorInfo数据更新到缓冲区域中，等待merge拉取
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
    //缓存中的monitor信息这个大字符串
    var bufferMonitorInfo1:String = buffer1.getString(0)
    //每一次传进来的monitor信息
    var bufferMonitorInfo2:String = buffer2.getString(0)
    // 把buffer2里面的数据都拆开来更新
    for ( monitorInfo:String <- bufferMonitorInfo2.split("\\|")){
      /**
        * monitor_id1 100
        * monitor_id2 88
        */
        //将每一次传进来的monitor信息转换成<k,v>对
      val map:Map[String, String] = StringUtils.getKeyValuesFromConcatString(monitorInfo, "\\|")
      //遍历map中的每一个元素产生entry
      for (entry:Tuple2[String,String] <- map){
        //遍历map中的每一个元素entry，key值为monitor，value值为carCount
        val monitor:String = entry._1
        val carCount:Int = entry._2.toInt
        //将缓存中的monitor信息大字符串提取字段值赋值给oldVS
        val oldVS = StringUtils.getFieldFromConcatString(bufferMonitorInfo1, "\\|", monitor)
        //如果从缓存中提取的字段值oldVS为空
        if (oldVS == null){
            //如果从缓存中提取的字段值oldVS为空且bufferMonitorInfo1也为空那么将传进来的monitor信息中对应字段和对应字段的value值直接放进bufferMonitorInfo1的缓存中
          if ("".equals(bufferMonitorInfo1)){
            bufferMonitorInfo1 += monitor + "=" + carCount
          }else{
            //如果从缓存中提取的字段值oldVS为空且bufferMonitorInfo1不为空那么将传进来的monitor信息中对应字段和对应字段的value值加竖划线放进bufferMonitorInfo1的缓存中
            bufferMonitorInfo1 += "|" + monitor + "=" + carCount
          }
        }else{
          //如果从缓存中提取的字段值oldVS不为空，那么将oldVS与传进来的monitor信息中对应字段的value值转成Int相加
          var oldVal:Int = oldVS.toInt
          oldVal += carCount
          //将从缓存中提取的字段值拼接的字符串
          bufferMonitorInfo1 = StringUtils.setFieldInConcatString(bufferMonitorInfo1, "\\|", monitor, oldVal+"")
        }
        //更新缓冲数据的值
        buffer1.update(0, bufferMonitorInfo1)
      }
    }
  }
  /**
    * evaluate方法返回数据的类型要和dateType的类型一致，不一致就会报错
    */
  //将缓冲数据返回
  override def evaluate(buffer: Row) = buffer.getString(0)
}

