def seq(counts_map:scala.collection.mutable.Map[String, Int], line:String):scala.collection.mutable.Map[String, Int]={
  line.split('\t')(121).split('|').foldLeft(counts_map){(map, word) => map + (word -> (map.getOrElse(word, 0) + 1))}
}
def comb(map1:scala.collection.mutable.Map[String, Int], map2:scala.collection.mutable.Map[String, Int]):scala.collection.mutable.Map[String, Int]={
  ( map1 /: map2 ) { case (map, (k,v)) => map + ( k -> (v + map.getOrElse(k, 0)) ) }
}
sc.textFile().treeAggregate(scala.collection.mutable.Map.empty[String, Int])(seq, comb, 2).foreach(println)


val raw_data = sc.textFile()
// raw_data.count 1264762
val littleData = raw_data.randomSplit(Array(0.1,0.9)).head
val cached_data = raw_data.repartition(300).cache
val splited_data = cached_data.flatMap(_.split('\t')(121).split('|'))
val counted_data = splited_data.countByValue




val raw_data = sc.textFile()
// raw_data.count 1264762
// val littleData = raw_data.randomSplit(Array(0.1,0.9)).head
// val cached_data = raw_data.repartition(300).cache
// val splited_data =
val counted_data = raw_data.flatMap(_.split('\t')(121).split('|')).map(x => x -> 1L).reduceByKey(_ + _) 
counted_data.saveAsTextFile()

case class Tag(similarTag : String)
val littleDF = littleData.map(Tag(_)).toDF
val t = raw_data.first.split('\t')(121).split('|')
t.map(x => (x,1).swap)


spark-submit
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 25G \
  --driver-cores 20 \
  --num-executors 300 \
  --executor-cores 20 \
  --executor-memory 25G \
  --class tag_count \
  spark-simple-package-1-8-0_2.11-0.1.0-SNAPSHOT.jar \
  /user/feed/warehouse/feed_sample_filter_v2/dt=20170712 \
  /user/feed/enzhao/tag_count/feed_sample_filter_v2/dt=20170712

