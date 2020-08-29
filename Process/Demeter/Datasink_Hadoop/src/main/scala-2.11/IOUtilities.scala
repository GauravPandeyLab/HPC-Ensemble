
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._

/**
  * Created by premtimsina on 3/31/17.
  * Includes Function for Read and Write
  */
class IOUtilities extends java.io.Serializable {
  val conf = ConfigFactory.load()

  /**
    * read data from CSV
    * @param spark
    * @param tableName
    * @return
    */
  def readCSV(spark: SparkSession, tableName: String): DataFrame = {
    val csvPath = conf.getString("csvDataSource.csvDatabase") + "/" + tableName + ".csv"
    val csvToDataFrame = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)
    csvToDataFrame
  }

  /**
    * write dataframe into csv format
    * @param spark
    * @param dataFrame
    * @param tableName
    * @param numOfPartition
    */
  def saveToCSV(spark: SparkSession, dataFrame: DataFrame, tableName: String, numOfPartition: Int = 1): Unit = {
    val path = conf.getString("csvDataSource.csvDatabase") + "/" + tableName + ".csv"
    dataFrame.repartition(numOfPartition).write.mode(SaveMode.Append)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(path)
  }

  /**
    * save dataframe into parquet file
    * @param sparkSession
    * @param df
    * @param tableName
    */
  def saveParquetFile(sparkSession: SparkSession, df: DataFrame, tableName: String): Unit = {
    val path = conf.getString("database.parquetDatabase") + tableName + ".parquet"
    df.repartition(1).write.mode(SaveMode.Overwrite).parquet(path)
  }

  /**
    * read data from parquet file
    * @param sparkSession
    * @param table
    * @return
    */
  def readParquetFile(sparkSession: SparkSession, table: String): DataFrame = {
    val path = conf.getString("database.parquetDatabase") + table + ".parquet"
    sparkSession.read.parquet(path)
  }

  /**
    * apend data into existing parquet file
    * @param sparkSession
    * @param df
    * @param tableName
    */
  def appendParquetFile(sparkSession: SparkSession, df: DataFrame, tableName: String): Unit = {
    val path = conf.getString("database.parquetDatabase") + tableName + ".parquet"
    df.repartition(1).write.mode(SaveMode.Append).parquet(path)
  }

}
