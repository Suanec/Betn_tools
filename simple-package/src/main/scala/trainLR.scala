object trainLR {
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  def main(args : Array[String]) = {
    println("args : path, iter")
    val conf = new SparkConf()
      .setAppName("trainLR")
    val sc_lr = new SparkContext(conf)
    println(sc_lr.getConf)

    val dataFile = sc_lr.textFile(args.head).persist
    val count = dataFile.count
    val iter = args(1).toInt
    println(s"path : ${args.head}, DataCount : ${count}")
    
    // Run training algorithm to build the model
    println("Training...")
    (0 to iter).map{
      iterNum =>
        val arr = 0 to Int.MaxValue - 1
        val data = sc_lr.parallelize(arr,3516*2)
        val result = data.mapPartitions{
          i =>
            for(k <- 0 to Int.MaxValue)
              Unit
            Array(i).iterator
        }.count
    }

    println("Trianing Done!")

    // Compute raw scores on the test set.
    println("Testing...")
    // result.count

    println("Testing Done!")

  }
}
