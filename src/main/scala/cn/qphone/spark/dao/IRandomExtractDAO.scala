package cn.qphone.spark.dao

import cn.qphone.spark.bean.{RandomExtractCar, RandomExtractMonitorDetail}

trait IRandomExtractDAO {
  def insertBatchRandomExtractCar(carRandomExtracts:List[RandomExtractCar] )

  def insertBatchRandomExtractDetails(r:List[RandomExtractMonitorDetail])
}
