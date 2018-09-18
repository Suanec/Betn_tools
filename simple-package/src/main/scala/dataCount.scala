/**
  * Created by enzhao on 2018/08/07.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object dataCount {
  def main(args : Array[String]) = {
    println("args : srcDir")
    val srcDir : String = args.head

    val appName = srcDir + " -- DataCount "
    val conf = new SparkConf().setAppName(appName)
    val sc_count = new SparkContext(conf)
    println(sc_count.getConf)

    val data = sc_count.textFile(srcDir)
    val srcCount = data.count
    val srcSizeCount = srcDir + "_HAS_" + srcCount
    println("Doing Count...")
    sc_count.parallelize(Array(srcCount)).saveAsTextFile(srcSizeCount)
    println("Count Done...")
  }
}


