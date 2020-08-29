


object PhaseIVEvaluation extends {

  import com.typesafe.config.ConfigFactory
  import org.apache.log4j.{Level, LogManager}
  import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
  import org.apache.spark.sql._


  /**
    * Created  on 3/30/17.
    */
  val conf = ConfigFactory.load()
  val utility = new IOUtilities()
  //  val service= new DataService()
  val featureEngineering = new FeatureEngineering()
  val mLModel = new MLModels()
  //  val mlModels = new MLModels()
  val modelEvaluator = new ModelEvaluator()

  def main(args: Array[String]) {
    //   ############################Use this conf when you run on local
    val sparkSession = SparkSession
      .builder()
      .appName("datasink")
      //        .enableHiveSupport()
      .config("spark.master", "local[*]")
      .config("spark.executor.memory", conf.getString("parameter.executorMemory"))
      .config("spark.driver.memory", "9g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047m")
      .config("spark.kryo.classesToRegister", "IOUtilities")
      .config("spark.sql.parquet.mergeSchema", "false")
      .config("spark.driver.maxResultSize", "0")
      .config("spark.default.parallelism", "200")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.speculation", "false")
      .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .getOrCreate()
    LogManager.getRootLogger.setLevel(Level.ERROR)



    LogManager.getRootLogger.setLevel(Level.ERROR)

    //    mLModel.evaluationBaseClassifierOuter(sparkSession,0)
    //    val df1= utility.readCSV(sparkSession,"cf_overall").repartition(1)
    //    utility.saveToCSV(sparkSession,df1, "confusionMatrixBaseClassifier",1)


    val finalResult = (for (j <- Array(0, 1, 2, 4)) yield {
      //           mLModel.evaluationBaseClassifier(sparkSession,j)

      //
      //overall AUC
      utility.readParquetFile(sparkSession, "finalResult" + j)
      //              sparkSession.read.parquet("/Users/premtimsina/PycharmProjects/Testing/parquetDatabase/"+"finalResult" + j+".parquet")
    }).reduce((a, b) => a.unionAll(b))
    //
    //
    val trainEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")
    val aucTotal = trainEvaluator.evaluate(finalResult)
    println("total value of AUC curve", aucTotal)
    //
    //
    //    val df1= utility.readCSV(sparkSession,"cf_overall").repartition(1)
    //    utility.saveToCSV(sparkSession,df1, "confusionMatrixBaseClassifier",1)
    sparkSession.stop()

    //        df.show()


    //    modelEvaluator.createCrossValidator(data,"r")


  }

}
