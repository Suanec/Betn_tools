/**
  * Created by enzhao on 2018/08/07.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object dataCount {
  def main(args : Array[String]) = {
    println("args : srcDir, isSaveRst")
    val srcDir : String = args.head

    val appName = srcDir + " -- DataCount "
    val conf = new SparkConf().setAppName(appName)
    val sc_count = new SparkContext(conf)
    println(sc_count.getConf)

    val isSaveRst : Boolean = args.size match {
      case 2 => args(1).toBoolean
      case _ => true
    }
    val data = sc_count.textFile(srcDir)
    println("Doing Count...")
    val srcCount = data.count
    println(s"src size count : ${srcCount} ")
    if(isSaveRst) {
      val srcSizeCount = srcDir + "_HAS_" + srcCount
      sc_count.parallelize(Array(srcCount)).saveAsTextFile(srcSizeCount)
    }
    println("Count Done...")
  }
}


