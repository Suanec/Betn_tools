/**
  * Created by enzhao on 2018/08/15.
  */


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object libsvmGenerator {
  def main(args : Array[String]) = {
    println("args : dataPath, sampleNum, featureSize, sparsity = 0.01, partitions = 100, isOnlyOne = true, format = libsvm[dummy]")
    if(args.size < 3) {
      sys.exit(1)
    }
    val dataPath: String = args.head
    val sampleNum: Long = args(1).toLong
    val featureSize: Long = args(2).toLong

    val sparsity: Double = (args.size >= 4) match {
      case true => args(3).toDouble
      case false => 0.01
    }
    val partitions: Int = (args.size >= 5) match {
      case true => args(4).toInt
      case false => 100
    }
    val isOnlyOne: Boolean = (args.size >= 6) match {
      case true => args(5).toBoolean
      case false => true
    }
    val format: String= (args.size >= 7) match {
      case true => args(6).toString
      case false => "libsvm"
    }
    val appName = dataPath + " -- libsvmGenerator"
    val conf = new SparkConf().setAppName(appName)
    val sc_libsvmGenerator = new SparkContext(conf)
    println(sc_libsvmGenerator.appName)
    println(sc_libsvmGenerator.applicationId)

    val b_rand = sc_libsvmGenerator.broadcast(new scala.util.Random())
    val b_size = sc_libsvmGenerator.broadcast((sampleNum / partitions.toDouble).ceil.toInt)
    val b_featureCount = sc_libsvmGenerator.broadcast((featureSize * sparsity).toInt)
    val b_featureSize = sc_libsvmGenerator.broadcast(featureSize)
    
    println(s"sample number in each partition. ${b_size.value}")
    println(s"sample values count in each sample. ${b_featureCount.value}")
    val partitionIndex = (0 until partitions).toArray
    val indexRdd = sc_libsvmGenerator.parallelize(partitionIndex,partitions)
    
    val sizeRdd = indexRdd.flatMap(x => (0 until b_size.value))

    val dummyRdd = sizeRdd.mapPartitions{
      iter =>
        iter.map(elem => (0 until b_featureCount.value).map(x => math.abs(b_rand.value.nextLong % b_featureSize.value)).distinct.sorted.tail)
    }

    val rstRdd = format match {
      case "libsvm" => 
        if(isOnlyOne) {
          dummyRdd.map{
            iter => 
              math.abs(b_rand.value.nextInt % 2).toString + " " + iter.map(x => x.toString + ":1.0").mkString(" ")
          }
        } else {
          dummyRdd.map{
            iter => 
              math.abs(b_rand.value.nextInt % 2).toString + " " + iter.map(x => x.toString + ":" + (b_rand.value.nextFloat % 10).toString.substring(0,6).mkString(" "))
          }
        }
      case "dummy" => 
          dummyRdd.map{
            iter => 
              math.abs(b_rand.value.nextInt % 2).toString + " " + iter.mkString(" ")
          }
      case _ => 
        println("args : dataPath, sampleNum, featureSize, sparsity = 0.01, partitions = 100, isOnlyOne = true, format = libsvm[dummy]")
        sys.exit(1)
    }
    
    val srcCount = b_size.value.toLong * partitions
    val srcSizeCount = dataPath + s"_HAS_${srcCount}_WITH_${sparsity}_IN_${featureSize}"
    println("Doing Count...")
    sc_libsvmGenerator.parallelize(Array(srcCount)).saveAsTextFile(srcSizeCount)
    println("Count Done...")
    println("Doing Generate...")
    rstRdd.saveAsTextFile(dataPath)
    println("Generate Done...")
  }
}



