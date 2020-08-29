object PhaseIIICombination extends {

  import com.typesafe.config.ConfigFactory
  import org.apache.log4j.{Level, LogManager}
  import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
  import org.apache.spark.sql._


  /**
    * Created  on 3/30/17.
    */
  val conf = ConfigFactory.load()
  val utility = new IOUtilities()
  val featureEngineering = new FeatureEngineering()
  val mLModel = new MLModels()

  def main(args: Array[String]) {

    LogManager.getRootLogger.setLevel(Level.ERROR)


    val sparkSession = SparkSession
      .builder()
      .appName("dataSink")
      .enableHiveSupport()
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


    val iteration = args(0).toInt
    val dir = args(1)
    val dataset_name = args(2)

    val df = mLModel.combination(sparkSession, iteration, dir, dataset_name)
    utility.saveParquetFile(sparkSession, df, dir + "/" + "finalResult" + iteration)

    val finalResult = (for (j <- 0 to 9) yield {
      sparkSession.read.parquet("/Users/premtimsina/PycharmProjects/Testing/parquetDatabase/" + "finalResult" + j + ".parquet")
    }).reduce((a, b) => a.unionAll(b))

    finalResult.show()

    val trainEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")
    val aucTotal = trainEvaluator.evaluate(finalResult)
    println("total value of AUC curve is nv", aucTotal)

    sparkSession.stop()

  }


}
