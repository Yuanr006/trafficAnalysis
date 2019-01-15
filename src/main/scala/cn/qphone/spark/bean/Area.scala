package cn.qphone.spark.bean

/**
  * @Type: ScalaBean
  * @Description: 区域类
  */

 class Area() {

  /**
    *  全参构造器
    * @param id 区域编号
    * @param name 区域名称
    */
   def this(id:String,name:String){
     this
     areaId=id
     areaName=name
   }

   /**
     * 区域编号
     */
   private[this] var _areaId: String = ""

   def areaId: String = _areaId

   def areaId_=(value: String): Unit = {
     _areaId = value
   }

   /**
     * 区域名称
     */
   private[this] var _areaName: String = ""

   def areaName: String = _areaName

   def areaName_=(value: String): Unit = {
     _areaName = value
   }
    //toString
   override def toString = s"Area( $areaId, $areaName)"
 }
