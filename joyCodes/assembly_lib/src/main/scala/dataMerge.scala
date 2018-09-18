/**
  * Created by enzhao on 2018/07/17.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object dataMerge {
  def main(args : Array[String]) = {
    println("args : srcDir, tarDir, outMapNum, mapNum")
    val srcDir : String = args.head
    val tarDir : String = args.tail.head
    val outMapNum : Int = (args.tail.size >= 3) match {
      case true => args.tail.tail.head.toInt
      case false => 2
    }
    val partitions : Int = args.size match {
      case 4 => args.last.toInt
      case _ => -1
    }

    if(srcDir.equals(tarDir)){
      println("ERROR : parameters Error!! src == tar")
      sys.exit(1)
    }
    val appName = srcDir + "-->" + tarDir + s" : with outMapNum ${outMapNum} by ${partitions}"
    val conf = new SparkConf().setAppName(appName)
    val sc_merge = new SparkContext(conf)
    println(sc_merge.getConf)

    val data = partitions match {
      case -1 => sc_merge.textFile( srcDir )
      case _ => sc_merge.textFile( srcDir, partitions )
    }
    // Run training algorithm to build the model
    println("Transing...")
    data.coalesce(outMapNum).saveAsTextFile(tarDir)
    println("Transing Done!")

  }
}


