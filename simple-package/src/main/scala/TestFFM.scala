import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD


object TestFFM extends App {

  override def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("TEST_FFM"))

    if (args.length != 8) {
      println("testFFM <train_file> <k> <n_iters> <eta> <lambda> " + "<normal> <random>")
    }

    val data= sc.textFile(args(0)).map(_.split("\\s")).map(x => {
      val y = if(x(0).toInt > 0 ) 1.0 else -1.0
      val nodeArray: Array[(Int, Int, Double)] = x.drop(1).map(_.split(":")).map(x => {
        (x(0).toInt, x(1).toInt, x(2).toDouble)
      })
      (y, nodeArray)
    })
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (training: RDD[(Double, Array[(Int, Int, Double)])], testing) = (splits(0), splits(1))

    //sometimes the max feature/field number would be different in training/testing dataset,
    // so use the whole dataset to get the max feature/field number
    val m = data.flatMap(x=>x._2).map(_._1).collect.reduceLeft(_ max _) //+ 1
    val n = data.flatMap(x=>x._2).map(_._2).collect.reduceLeft(_ max _) //+ 1

    val ffm: FFMModel = FFMWithAdag.train(training, m, n, dim = (args(6).toBoolean, args(7).toBoolean, args(1).toInt), n_iters = args(2).toInt,
      eta = args(3).toDouble, regParam = (args(4).toDouble, args(5).toDouble), normalization = false, false, "adagrad")

    val scoreAndLabels = testing.map(x => {
      val p = ffm.predict(x._2)
      (p, x._1)
    })
    val scores: RDD[(Double, Double)] = testing.map(x => {
      val p = ffm.predict(x._2)
      val ret = if (p >= 0.5) 1.0 else -1.0
      (ret, x._1)
    })

    val testMetrics = new BinaryClassificationMetrics(scoreAndLabels)
    val testAUC     = testMetrics.areaUnderROC()


    val accuracy = scores.filter(x => x._1 == x._2).count().toDouble / scores.count()
    println(s"accuracy = $accuracy")
    println(s"auc = $testAUC")
  }
}

