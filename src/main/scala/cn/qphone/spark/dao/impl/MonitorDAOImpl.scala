package cn.qphone.spark.dao.impl

import cn.qphone.spark.bean.CarTrack

trait ICarTrackDAO {
 def insertBatchCarTrack(carTracks:List[CarTrack] )
}
