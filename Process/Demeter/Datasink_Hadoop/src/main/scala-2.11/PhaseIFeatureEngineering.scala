import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql._


/**
  * Created  on 3/30/17.
  * It performs the data cleaning
  */
object PhaseIFeatureEngineering extends java.io.Serializable {
  val conf = ConfigFactory.load()
  val utility = new IOUtilities()
  val featureEngineering = new FeatureEngineering()

  def main(args: Array[String]) {


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


    val fileName = args(0)
    val directoryName = args(1)
    val dir1 = new File("parquetDatabase/" + directoryName)
    val dir2 = new File("parquetDatabase/" + directoryName + "/Result")
    val dir3 = new File("parquetDatabase/" + directoryName + "/ResultCache")

    dir1.mkdir()
    dir2.mkdir()
    dir3.mkdir()


    val data = utility.readCSV(sparkSession, fileName)

    //    modelEvaluator.createCrossValidator(data,"r")


    val inputCols = data.columns.filter(_ != "label").filter(_ != "SN")
    import sparkSession.implicits._

    val dataWithFeature = featureEngineering.featureAssembler(sparkSession, sparkSession.sparkContext, inputCols, "features", data).select($"SN", $"features", $"label".cast("double") as ("label"))
    //
    val df = featureEngineering.scalar0_1(sparkSession, sparkSession.sparkContext, "features", "scaledFeatures", dataWithFeature)
      .select($"SN", $"scaledFeatures" as ("features"), $"label".cast("double") as ("label")).cache()
    featureEngineering.crossValidatorSplit(df, "outer", directoryName) //outercrossvalidation
    //    "parquetDatabase/"+dir+"/"+outer
    for (i <- 0 to 4) {
      val df = utility.readParquetFile(sparkSession, directoryName + "/outer/training" + i)
      featureEngineering.crossValidatorSplit(df, i.toString, directoryName)
    }
    sparkSession.stop()

  }
}
