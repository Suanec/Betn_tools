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


// SPARK_BIN="spark-submit"
// TARGET_JAR="spark-tools-1-8-0_2.11-0.1.0-SNAPSHOT.jar"
// MASTER_URL="yarn"
// DEPLOY_MODE="cluster"
// NUM_EXECUTORS="16"
// DRIVER_MEMORY="1500m"
// EXECUTOR_CORES="1"
// EXECUTOR_MEMORY="900m"
// QUEUE_NAME="ml_cc"
// CLASS_NAME="transData"
// SRC_DIR="hdfs://10.87.49.220:8020/user/push_weibo/temp_cuiwei_push_log_singlegroup_distinct_dt/000026_0"
// TAR_DIR="temp_cuiwei_push_log_singlegroup_distinct_dt/000026_0"
// MAP_NUM=80
// config="
//    ${SPARK_BIN} \
//    --master ${MASTER_URL} \
//    --deploy-mode ${DEPLOY_MODE} \
//    --num-executors ${NUM_EXECUTORS} \
//    --driver-memory ${DRIVER_MEMORY} \
//    --executor-cores ${EXECUTOR_CORES} \
//    --executor-memory ${EXECUTOR_MEMORY} \
//    --queue ${QUEUE_NAME} \
//    --conf spark.ui.retainedJobs=2 \
//    --conf spark.ui.retainedStages=2 \
//    --conf spark.worker.ui.retainedExecutors=5 \
//    --conf spark.worker.ui.retainedDrivers=5 \
//    --conf spark.eventLog.enabled=false \
//    --class ${CLASS_NAME} \
//    ${SRC_DIR} ${TAR_DIR} ${MAP_NUM}
// "
// 
// SPARK_BIN="spark-submit"
// TARGET_JAR="spark-tools-1-8-0_2.11-0.1.0-SNAPSHOT.jar"
// MASTER_URL="yarn"
// DEPLOY_MODE="cluster"
// NUM_EXECUTORS="17"
// DRIVER_MEMORY="1500m"
// EXECUTOR_CORES="8"
// EXECUTOR_MEMORY="11g"
// QUEUE_NAME="ml_cc"
// CLASS_NAME="trainLRm"
// ITER_NUM="192"
// APP_NAME="lr-from-tiramisu-train"
// 
// config="
//    $SPARK_BIN \
//    --master $MASTER_URL \
//    --deploy-mode ${DEPLOY_MODE} \
//    --name ${APP_NAME} \
//    --queue ${QUEUE_NAME} \
//    --num-executors ${NUM_EXECUTORS} \
//    --driver-memory ${DRIVER_MEMORY} \
//    --executor-cores ${EXECUTOR_CORES} \
//    --executor-memory ${EXECUTOR_MEMORY} \
//    --conf spark.ui.retainedJobs=2 \
//    --conf spark.ui.retainedStages=2 \
//    --conf spark.worker.ui.retainedExecutors=5 \
//    --conf spark.worker.ui.retainedDrivers=5 \
//    --conf spark.eventLog.enabled=false \
//    --class ${CLASS_NAME} \
//    $TARGET_JAR \
//    $ITER_NUM $APP_NAME
// "
// 
// date1=`date +%F_%H-%M-%S`
// $config
// 
// date2=`date +%F_%H-%M-%S`
// echo date start : $date1
// echo data size : $trainpath
// echo $iterNum
// echo $config
// echo date ended : $date2
