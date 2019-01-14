package cn.qphone.spark.bean

object Test {
  def main(args: Array[String]): Unit = {
    val area = new Area {}

    area.setAreaId("s")
    print(area.getAreaId)
    area.setAreaName("张三")
    print(area.getAreaName)

  }
}
