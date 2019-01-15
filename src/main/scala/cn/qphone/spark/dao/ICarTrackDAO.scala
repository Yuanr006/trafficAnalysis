package cn.qphone.spark.dao

import cn.qphone.spark.bean.CarTrack

trait ICarTrackDAO {
 def insertBatchCarTrack(carTracks:List[CarTrack] )
}
