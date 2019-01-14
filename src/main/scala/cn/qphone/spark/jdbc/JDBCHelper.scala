package cn.qphone.jdbc

import java.sql.{Connection, DriverManager, ResultSet}
import java.util
import java.util.Properties

/**
  * 连接数据库增删改查
  * @author xjw
  *
  */
object JDBCHelper extends {
  val props = getProperties()
  private val max = Integer.parseInt(props.getProperty("jdbc.datasource.size")) //连接池连接总数
  private val connectionNum = Integer.parseInt(props.getProperty("jdbc.initconnection.num")) //每次产生连接数
  private val pool = new util.LinkedList[Connection]() //连接池
  private var conNum = 0 //当前连接池已产生的连接数

  //获取连接
  def getJdbcConn(): Connection = {
    //同步代码块
    AnyRef.synchronized({
      if (pool.isEmpty) {
        //加载驱动
        preGetConn()
        for (i <- 1 to connectionNum) {
          val conn = DriverManager.getConnection(props.getProperty("jdbc.url"), props.getProperty("jdbc.user"), props.getProperty("jdbc.password"))
          pool.push(conn)
          conNum += 1
        }
      }
      pool.poll()
    })
  }//释放连接
  def releaseConn(conn: Connection): Unit = {
    pool.push(conn)
  }
  //加载驱动
  private def preGetConn(): Unit = {
    //控制加载
    if (conNum < max && !pool.isEmpty) {
      println("Jdbc Pool has no connection now, please wait a moments!")
      Thread.sleep(2000)
      preGetConn()
    } else {
      Class.forName(props.getProperty("jdbc.driver"));
    }
  }
  //获取配置文件
  def getProperties(): Properties ={
    val props = new Properties()
    val in=this.getClass.getClassLoader.getResourceAsStream("my.properties")
    props.load(in)
    props
  }
  /**
    * 执行增删改SQL语句，返回影响的行数
    * @param sql
    * @param params
    * @return 影响的行数
    */
  def executeUpdate(sql:String,params:Array[Object]): Int ={
    val conn = getJdbcConn()
    conn.setAutoCommit(false)
    val prep = conn.prepareStatement(sql)
    if(params != null && params.length > 0) {
      for(i <- 0 to params.length-1) {
        prep.setObject(i+1,params(i))
      }
    }
    val rtn = prep.executeUpdate()
    conn.commit()
    releaseConn(conn)
    rtn
  }
  def executeQuery(sql:String,params:Array[String]): ResultSet ={
    val conn = getJdbcConn()
    conn.setAutoCommit(false)
    val prep = conn.prepareStatement(sql)
    if(params != null && params.length > 0) {
      for(i <- 0 to params.length-1) {
        prep.setObject(i+1,params(i))
      }
    }
    val rtn = prep.executeQuery()
    conn.commit()
    releaseConn(conn)
    rtn
  }

  def executeBatch(sql:String,paramsList:List[Array[Object]]): Unit ={
    val conn = getJdbcConn()
    conn.setAutoCommit(false)
    val prep = conn.prepareStatement(sql)
    if(paramsList!=null && paramsList.length>0){
      for (params <- paramsList) {
        for(i <- 0 to params.length-1) {
          prep.setObject(i+1,params(i))
        }
        prep.addBatch()
      }
      val rtn = prep.executeBatch()
      conn.commit()
      releaseConn(conn)
      rtn
    }
  }
}

