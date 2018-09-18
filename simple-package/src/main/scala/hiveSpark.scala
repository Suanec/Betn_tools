/**
  * Created by enzhao on 2018/08/07.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object hiveSpark {
  def main(args : Array[String]) = {

    val appName = " -- hive_on_spark"
    val conf = new SparkConf().setAppName(appName)
    val sc_hive = new SparkContext(conf)

    println("hive querying...")
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc_hive)
    import hiveContext.implicits._
    hiveContext.sql("use public")
    hiveContext.sql("show tables").show

    println("query done...")
  }
}


