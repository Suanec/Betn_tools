
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.MLUtils


/**
 * Created by zrf on 4/18/15.
 */


object TestFM extends App {

  override def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("TESTFM"))

    if (args.length > 0) {
      println("testFM <train_file> <k> <n_iters> <eta> <lambda> " + "<normal> <random>")
    }

    val dataPath = args(0)
    val k = args(1).toInt
    val iters = args(2).toInt
    //    "hdfs://ns1/whale-tmp/url_combined"
    val training = MLUtils.loadLibSVMFile(sc, dataPath)

    val fm1 = FMWithSGD.train(training, task = 1, numIterations = iters, stepSize = 0.15, miniBatchFraction = 0.0000001, dim = (true, true, k), regParam = (0, 0, 0), initStd = 0.1)


    // val fm2 = FMWithLBFGS.train(training, task = 1, numIterations = 20, numCorrections = 5, dim = (true, true, 4), regParam = (0, 0, 0), initStd = 0.1)
    
  }
}
