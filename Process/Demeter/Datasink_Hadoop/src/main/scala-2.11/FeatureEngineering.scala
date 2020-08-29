import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel._


/**
  * Created by premtimsina on 4/4/17.
  * The class includes functions for diffrent feature engineering technique
  */
class FeatureEngineering extends java.io.Serializable {
  val conf = ConfigFactory.load()
  val trainingSize = conf.getDouble("parameter.trainingSize")
  val testingSize = conf.getDouble("parameter.testingSize")
  val utility = new IOUtilities()

  /**
    *
    * @param sparkSession
    * @param sc
    * @param inputCols
    * @param outputCol
    * @param data
    * @return
    *  it assembles input features into Vector dataformat
    */
  def featureAssembler(sparkSession: SparkSession, sc: SparkContext, inputCols: Array[String], outputCol: String, data: DataFrame): DataFrame = {

    data.persist(MEMORY_AND_DISK)
    val assemblerModel = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")
    val outputDF = assemblerModel.transform(data)
    data.unpersist(true)
    outputDF
  }

  /**
    * split dataframe into train and test
    * @param sparkSession
    * @param dataFrame
    * @return
    */
  def TrainTestSplit(sparkSession: SparkSession, dataFrame: DataFrame): (DataFrame, DataFrame) = {
    val positiveDF = dataFrame.where("label =1.0")
    val negativeeDF = dataFrame.where("label =0.0")
    val posDF: Array[DataFrame] = positiveDF.randomSplit(Array(trainingSize, testingSize), seed = 7777)
    val negDF: Array[DataFrame] = negativeeDF.randomSplit(Array(trainingSize, testingSize), seed = 7777)
    val trainingDF = posDF(0).union(negDF(0))
    val testingDF = posDF(1).union(negDF(1))
    (trainingDF, testingDF)
  }

  /**
    * scale feature vector into 0 and 1 scale
    * @param sparkSession
    * @param sc
    * @param inputcol
    * @param outputcol
    * @param data
    * @return
    */
  def scalar0_1(sparkSession: SparkSession, sc: SparkContext, inputcol: String, outputcol: String, data: DataFrame): DataFrame = {
    val scalarPlan = new MinMaxScaler().setInputCol(inputcol).setOutputCol(outputcol)
    val scalar0_1Model = scalarPlan.fit(data)
    val scaledDataFrame = scalar0_1Model.transform(data)
    scaledDataFrame
  }

  def crossValidatorSplit(df: DataFrame, outer: String = "", dir: String): Unit = {
    import java.io.File
    val path = "parquetDatabase/" + dir + "/" + outer
    if (!new java.io.File(path).exists) {
      val dir = new File(path)
      dir.mkdir()
    }
    val sn = df.select("SN")
    val dfArr = df.randomSplit(Array.fill[Double](5)(0.2))
    (0 to 4).toArray.map { i =>
      val training = df.except(dfArr(i))
      val testing = dfArr(i)

      training.write.parquet(path + "/training" + i + ".parquet")
      testing.write.parquet(path + "/testing" + i + ".parquet")
    }

  }


}
