import cn.qphone.spark.conf.ConfigurationManager
import cn.qphone.spark.constant.Constants
//import com.alibaba.fastjson.{JSON, JSONObject}


/**
  * 参数工具类，json解析
  * @author Administrator
  *
  */
//object ParamUtils extends Constants{
//  /**
//    * 从命令行参数中提取任务id
//    * @param args 命令行参数
//    * @param taskType 参数类型(任务id对应的值是Long类型才可以)，对应my.properties中的key
//    * @return 任务id
//    */
//  def getTaskIdFromArgs(args:Array[String],taskType:String): Long ={
//    val local = ConfigurationManager.getBoolean(SPARK_LOCAL)
//    if(local) {
//      ConfigurationManager.getLong(taskType)
//    } else {
//        if(args != null && args.length > 0) {
//           String.valueOf(args(0)).toLong
//        }else 0
//    }
//  }
//  /**
//    * 从JSON对象中提取参数
//    * @param jsonObject JSON对象
//    * @return 参数
//    * {"name":"zhangsan","age":"18"}
//    */
//  def getParam(jsonObject:JSONObject,field:String): String  ={
//    val jsonArray = jsonObject.getJSONArray(field)
//    if(jsonArray != null && jsonArray.size() > 0) {
//       jsonArray.getString(0)
//    }else null
//  }
//}