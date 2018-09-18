/**
  * Created by enzhao on 2017/11/20.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object transData {
  def main(args : Array[String]) = {
    println("args : srcDir, tarDir, mapNum")
    val srcDir : String = args.head
    val tarDir : String = args.tail.head
    val partitions : Int = args.size match {
      case 3 => args.last.toInt
      case _ => -1
    }
    if(srcDir.equals(tarDir)){
      println("ERROR : parameters Error!! src == tar")
      sys.exit(1)
    }
    val appName = srcDir + "-->" + tarDir + s" : with ${partitions}"
    val conf = new SparkConf().setAppName(appName)
    val sc_trans = new SparkContext(conf)
    println(sc_trans.getConf)

    val data = partitions match {
      case -1 => sc_trans.textFile( srcDir )
      case _ => sc_trans.textFile( srcDir, partitions )
    }
    val count = data.count
    // Run training algorithm to build the model
    println("Transing...")
    data.saveAsTextFile(tarDir)
    println("Transing Done!")

  }
}


