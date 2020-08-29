import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{collect_list, lit, udf}

import scala.collection.mutable

/**
  * Created by premtimsina on 9/27/17.
  */
object PhaseIIModelingOuter extends {

  import com.typesafe.config.ConfigFactory
  import org.apache.log4j.{Level, LogManager}
  import org.apache.spark.sql._


  /**
    * Created  on 3/30/17.
    * Pipeline for training Outer model as described in Datasink algorithm
    */
  val conf = ConfigFactory.load()
  val utility = new IOUtilities()
  val featureEngineering = new FeatureEngineering()
  val mLModel = new MLModels()

  def main(args: Array[String]) {


    val sparkSession = SparkSession
      .builder()
      .appName("dataSink")
      //            .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.kryoserializer.buffer.max", "1g")
      .config("spark.sql.parquet.mergeSchema", "false")
      .config("spark.memory.fraction", "0.8")
      .config("spark.driver.maxResultSize", "0")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.speculation", "false")
      .config("spark.executor.heartbeatInterval", "20s")
      .config("spark.network.timeout", "200s")
      .getOrCreate()
    LogManager.getRootLogger.setLevel(Level.ERROR)
    val toVector = udf(convertToVector(_: mutable.WrappedArray[Double]))


    val modelName = args(0)
    val iteration = args(1)
    val outerIteration = args(2)
    val dir = args(3)
    println(modelName, "   ", iteration, "   ", outerIteration)
    val training = utility.readParquetFile(sparkSession, dir + "/" + outerIteration + "/training" + iteration).cache()
    val testing = utility.readParquetFile(sparkSession, dir + "/" + outerIteration + "/testing" + iteration).cache()

    val trPos = training.where("label=1").cache()
    val trNeg = training.where("label=0").cache()
    val posC = trPos.count().toDouble
    val negC = trNeg.count().toDouble
    val ratio = posC / negC

    for (itr <- 0 to 9) {
      val training1 = (trNeg.sample(false, ratio, itr.toLong)).unionAll(trPos)
      val df = mLModel.trainModel(sparkSession, training1, testing, modelName)
        .withColumnRenamed("probability", modelName)
        .withColumn("iteration", lit(itr))


      utility.appendParquetFile(sparkSession, df, dir + "/" + "ResultCache/" + modelName + outerIteration + iteration)
    }

    val df1 = utility.readParquetFile(sparkSession, dir + "/" + "ResultCache/" + modelName + outerIteration + iteration)
      .select("SN", modelName, "iteration")
    val df2 = df1.orderBy("SN", "iteration").groupBy("SN").agg(collect_list(df1(modelName)) as modelName)
    val df3 = df2.withColumn(modelName + "1", toVector(df2(modelName)))
      .drop(modelName)
      .withColumnRenamed(modelName + "1", modelName)

    utility.saveParquetFile(sparkSession, df3, dir + "/" + modelName + outerIteration + iteration)

    //        df.show()


    //    modelEvaluator.createCrossValidator(data,"r")

    sparkSession.stop()


  }

  def convertToVector(arr: mutable.WrappedArray[Double]) = {
    Vectors.dense(arr.toList.toArray)
  }

}
