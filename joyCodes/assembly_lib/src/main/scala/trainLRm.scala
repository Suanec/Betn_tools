object trainLRm {
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  def main(args : Array[String]) = {
    println("args : iter, AppName")
    val conf = new SparkConf().setAppName("mem_run")
	if(args.size == 2) conf.setAppName(args.tail.head)
    val sc_lr = new SparkContext(conf)
    println(sc_lr.getConf)

    val data = sc_lr.parallelize((0 to 1023).toSeq,1023) 
	val count = data.count
    val iter = args.head.toInt
    println(s"iter : ${args.head}, DataCount : ${count}")
    // Run training algorithm to build the model
    println("Training...")
    val rand = new util.Random()
    val mem = Array.fill[
      org.apache.spark.rdd.RDD[
        scala.collection.immutable.IndexedSeq[
          Double]]](iter)(
      data.map(x => (0 to 36535 )
        .map(i => rand.nextDouble)).cache )
    mem.map(_.count).foreach(println)

    println("Trianing Done!")

    // Compute raw scores on the test set.
    println("Testing...")
    // result.count
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

    println("Testing Done!")

  }
}
// spark-shell  --master yarn  --num-executors 8 --driver-memory 3g --executor-cores 1 --executor-memory 20g


