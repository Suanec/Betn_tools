object tag_count {
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  def main(args : Array[String]) = {
    println("args : path, save_path")
	if(args.size != 2) {
	    println("args : path, save_path")
		System.exit(1)
	}
    val conf = new SparkConf()
      .setAppName("tag_count")
    val sc_tc = new SparkContext(conf)

    val raw_data = sc_tc.textFile(args.head)
    // raw_data.count 1264762
    // val littleData = raw_data.randomSplit(Array(0.1,0.9)).head
    // val cached_data = raw_data.repartition(300).cache
    // val splited_data =
    val counted_data = raw_data.flatMap(_.split('\t')(121).split('|')).map(x => x -> 1L).reduceByKey(_ + _) 
    counted_data.saveAsTextFile(args.last)

  }
}
