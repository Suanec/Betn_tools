/**
  * Created by enzhao on 2018/07/17.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object dataCopy {
  def main(args : Array[String]) = {
    println("args : srcDir, tarDir, factor, mapNum")
    val srcDir : String = args.head
    val tarDir : String = args.tail.head
    val factor : Int = (args.tail.size >= 3) match {
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
    val appName = srcDir + "-->" + tarDir + s" : with factor ${factor} by ${partitions}"
    val conf = new SparkConf().setAppName(appName)
    val sc_copy = new SparkContext(conf)
    println(sc_copy.getConf)

    val data = partitions match {
      case -1 => sc_copy.textFile( srcDir )
      case _ => sc_copy.textFile( srcDir, partitions )
    }
    val srcCount = data.count
    val srcSizeCount = srcDir + "_HAS_" + srcCount
    sc_copy.parallelize(Array(srcCount)).saveAsTextFile(srcSizeCount)
    // Run training algorithm to build the model
    println("Doing Union...")
    // val unionData = (0 until factor).map(x => data).reduce((x,y) => sc_copy.union(x,y))
    val unionData = (0 until factor).foldLeft(data)((x,y) => sc_copy.union(x,data))
    // val unionData = sc_copy.union(data,data)
    println("Union Done...")
    println("Transing...")
    unionData.saveAsTextFile(tarDir)
    println("Transing Done!")
    val tarCount = unionData.count
    val tarSizeCount = tarDir + "_HAS_" + tarCount
    sc_copy.parallelize(Array(tarCount)).saveAsTextFile(tarSizeCount)

  }
}


