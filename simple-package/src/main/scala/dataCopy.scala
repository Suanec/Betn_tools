/**
  * Created by enzhao on 2018/07/17.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object dataCopy {
  def main(args : Array[String]) = {
    println("args : srcDir, tarDir, factor, mapNum, isCount")
    val srcDir : String = args.head
    val tarDir : String = args.tail.head
    val factor : Int = (args.tail.size >= 3) match {
      case true => args(2).toInt
      case false => 2
    }
    val partitions : Int = (args.size >= 4) match {
      case true => args(3).toInt
      case false => -1
    }
    val isCount : Boolean = (args.size >= 5) match {
      case true => args(4).toBoolean
      case false => true
    }

    if(srcDir.equals(tarDir)){
      println("ERROR : parameters Error!! src == tar")
      sys.exit(1)
    }
    val appName = srcDir + "-->" + tarDir + s" : with factor ${factor} by ${partitions}"
    val conf = new SparkConf().setAppName(appName).set("spark.driver.maxResultSize", "20g")

    val sc_copy = new SparkContext(conf)
    sc_copy.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    println(sc_copy.getConf)
    println(args.mkString("\t --> \t"))

    val data = partitions match {
      case -1 => sc_copy.textFile( srcDir )
      case _ => sc_copy.textFile( srcDir, partitions )
    }
    if(isCount) {
      val srcCount = data.count
      val srcSizeCount = srcDir + "_HAS_" + srcCount
      sc_copy.parallelize(Array(srcCount)).saveAsTextFile(srcSizeCount)
    }
    // Run training algorithm to build the model
    println("Doing Union...")
    // val unionData = (0 until factor).map(x => data).reduce((x,y) => sc_copy.union(x,y))
    val unionData = (0 until factor).foldLeft(data)((x,y) => sc_copy.union(x,data))
    // val unionData = sc_copy.union(data,data)
    println("Union Done...")
    println("Transing...")
    unionData.coalesce(partitions).saveAsTextFile(tarDir)
    println("Transing Done!")
    if(isCount) {
      val tarCount = unionData.count
      val tarSizeCount = tarDir + "_HAS_" + tarCount
      sc_copy.parallelize(Array(tarCount)).saveAsTextFile(tarSizeCount)
    }
  }
}


