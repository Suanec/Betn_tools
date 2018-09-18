/**
  * Created by enzhao on 2018/08/11.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object dataQuartile{
  def main(args : Array[String]) = {
    println("args : srcDir, metaIdx, isDistinct, fieldDelimiter")
    if(args.size != 3 && args.size != 4) sys.exit(args.size)
    val srcDir : String = args.head
    val metaIdx : Int = args(1).toInt
    val isDistinct : Boolean = args(2).toBoolean
    val fieldDelimiter : String = (args.size == 4) match {
      case true => args(3)
      case false => "\t"
    }

    val appName = srcDir + " -- quartile"
    val conf = new SparkConf().setAppName(appName)
    val sc_quartile = new SparkContext(conf)
    println(sc_quartile.getConf)

    val data = sc_quartile.textFile(srcDir).map(x => x.split(fieldDelimiter)(metaIdx).toDouble)
    val rdd = isDistinct match {
      case true => data.distinct.sortBy(identity).zipWithIndex.map(_.swap)
      case false => data.sortBy(identity).zipWithIndex.map(_.swap)
    }
    val rddSize = rdd.count
    val rstIdx = 0L +: (0.01 to 0.99 by 0.01).map(x => (rddSize * x).ceil.toLong)
    val b_rstIdx = sc_quartile.broadcast(rstIdx)
    val rstPair = rdd.filter( pair => b_rstIdx.value.contains(pair._1) )
    // val rstRDD = sc_quartile.parallelize(rstPair,1)
    val rstRDD = rstPair.sortByKey()
    val rstRDDPATH = srcDir + s"_QUARTILES_WITH_${metaIdx}_${isDistinct}" 
    println("Doing Count...")
    rstRDD.saveAsTextFile(rstRDDPATH)
    println("Count Done...")
  }
}


