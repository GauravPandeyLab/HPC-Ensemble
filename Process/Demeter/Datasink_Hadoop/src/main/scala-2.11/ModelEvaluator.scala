import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
  * Created  on 9/27/17.
  * Functions for model evaluations
  */
class ModelEvaluator extends java.io.Serializable {
  val utility = new IOUtilities()

  /**
    * generate performace metrics from the prediction and label column
    * @param spark
    * @param df
    * @param modelName
    * @param iteration
    * @param outerIter
    */
  def createConfusionMatrix(spark: SparkSession, df: DataFrame, modelName: String, iteration: Long = 0, outerIter: Int = 0): Unit = {
    import spark.implicits._


    val df1 = df.withColumn("predictionMatch", when($"label" === 0 and $"prediction" === 0, "TN")
      .when($"label" === 1 and $"prediction" === 1, "TP")
      .when($"label" === 0 and $"prediction" === 1, "FP")
      .when($"label" === 1 and $"prediction" === 0, "FN")
      .otherwise("Error"))
    df1.cache()
    val trainEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")
    val aucTotal = trainEvaluator.evaluate(df1)


    val confusionMat = df1.select("predictionMatch").groupBy("predictionMatch").count().withColumnRenamed("count", modelName)
    val confArray1 = confusionMat.rdd.map { row => {
      row.getAs[String]("predictionMatch")
    }
    }.collect()
    val confArray2 = confusionMat.rdd.map { row => {
      row.getAs[Long](modelName)
    }
    }.collect()
    val mapC = confArray1.zip(confArray2).toMap
    val x: (Long, Long, Long, Long, Double, Double, Double) = (Try(mapC("TN")).getOrElse(0), Try(mapC("FN")).getOrElse(0), Try(mapC("FP")).getOrElse(0), Try(mapC("TP")).getOrElse(0),
      Try(mapC("TP") / (mapC("TP") + mapC("FN")).toDouble).getOrElse(0.0), Try(mapC("TP") / (mapC("TP") + mapC("FP")).toDouble).getOrElse(0.0).toString.toDouble, aucTotal)
    val confusionDF = spark.sparkContext.parallelize(Array(x)).toDF("TN", "FN", "FP", "TP", "Recall", "Precision", "aucROC")
      .withColumn("model", lit(modelName))
    //      .withColumn("outerIter", lit(outerIter))
    utility.saveToCSV(spark, confusionDF, "cf_overall", 1)
    confusionDF.show()
    df1.unpersist()

    //    confusionDF.show()

  }
}
