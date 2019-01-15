package cn.qphone.spark.dao

import cn.qphone.spark.bean.Area

/**
  * @Auther: zhuguangyuan 1159814737@qq.com
  * @Date: 2019/1/14 22:23
  * @Description: ${Description}
  */
trait IAreaDao {
  def findAreaInfo():List[Area]
}
