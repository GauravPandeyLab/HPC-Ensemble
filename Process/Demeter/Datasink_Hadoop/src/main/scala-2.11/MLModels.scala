import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel._

/**
  * Created  on 4/4/17.
  * The class has boiler plate for diffrent Ml algorithms
  */
class MLModels extends java.io.Serializable {
  val svmProb = udf(calculateSVMProbability(_: org.apache.spark.ml.linalg.Vector))
  val conf = ConfigFactory.load()
  val featureEngineering = new FeatureEngineering()
  val evaluator = new ModelEvaluator()
  val utility = new IOUtilities()
  val predUDF = udf(extractPrediction(_: org.apache.spark.ml.linalg.Vector))

  /**
    * greate GBT classifier
    * @return
    */
  def gbtClassifier(): GBTClassifier = {
    val dt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
    dt
  }

  /**
    * create sgd classifier
    * @param sparkSession
    * @return
    */
  def sgd(sparkSession: SparkSession): LogisticRegressionWithSGD = {
    val dt = new LogisticRegressionWithSGD()

    dt
  }

  /**
    * train machine learning model
    * @param spark
    * @param tr
    * @param testing
    * @param modelType
    * @return
    */
  def trainModel(spark: SparkSession, tr: DataFrame, testing: DataFrame, modelType: String): DataFrame = {
    val training = tr.persist(MEMORY_AND_DISK)
    val resultDF = modelType match {
      case "randomForest" => {
        val model1 = randomForestModel()

        val calModel = model1.fit(training)
        val result = calModel.transform(testing)
        //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
        val result1 = result.select(result("SN"), predUDF(result("probability")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))

        result1

      }
      case "naivebayes" => {
        val model1 = naiveBayes()
        val calModel = model1.fit(training)
        val result = calModel.transform(testing)
        //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
        val result1 = result.select(result("SN"), predUDF(result("probability")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))

        result1
      }
      case "logistic" => {
        val model1 = logisticRegression()

        val calModel = model1.fit(training)
        val result = calModel.transform(testing)
        //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
        val result1 = result.select(result("SN"), predUDF(result("probability")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))

        result1

      }
      case "perceptron" => {
        val model1 = perceptron()

        val calModel = model1.fit(training)
        val result = calModel.transform(testing)
        //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
        val result1 = result.select(result("SN"), result("prediction") as ("probability"), result("label"), result("prediction"))
        result1


      }

      case "svm" => {
        val model1 = svmModel()
        val calModel = model1.fit(training)
        val result = calModel.transform(testing)
        //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
        val result1 = result.select(result("SN"), svmProb(result("rawPrediction")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))
        result1

      }

      case "decisiontree" => {
        val model1 = decisionTree()
        val calModel = model1.fit(training)
        val result = calModel.transform(testing)
        //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
        val result1 = result.select(result("SN"), predUDF(result("probability")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))

        result1


      }


      case _ => spark.emptyDataFrame

    }
    resultDF

  }

  /**
    * create perceptron classifier
    * @return
    */
  def perceptron(): MultilayerPerceptronClassifier = {
    val layers = Array[Int](9, 5, 4, 2)
    val perceptron = new MultilayerPerceptronClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
    perceptron
  }

  /**
    * extract positive class prediction score
    * @param pred
    * @return
    */
  def extractPrediction(pred: org.apache.spark.ml.linalg.Vector): Double = {
    val pred1 = pred(1)
    pred1
  }

  //def createCompositeClassifier[T](sparkSession: SparkSession, df: DataFrame, model: T, modelType: String): Unit = {
  def createCompositeClassifier(sparkSession: SparkSession, df: DataFrame): Unit = {

    df.persist(MEMORY_AND_DISK)
    val snOuterDF = df.select("SN")
    snOuterDF.persist(MEMORY_AND_DISK)
    //splitting for outer 10 cross validation
    val snOuterDF1: Array[DataFrame] = snOuterDF.randomSplit(Array.fill[Double](10)(0.1))
    for (i <- 0 to 9) {
      val snTr = snOuterDF.except(snOuterDF1(i))
      val training = df.join(snTr, "SN")
      val testing = df.join(snOuterDF1(i), "SN")
      training.persist(MEMORY_AND_DISK)
      testing.persist(MEMORY_AND_DISK)
      //          training.show()

      //for undersampling
      val trPos = training.where("label=1")
      val trNeg = training.where("label=0")
      val posC = trPos.count()
      val negC = trNeg.count()
      val training1 = if (negC > posC) {
        trNeg.limit(posC.toInt).union(trPos)
      } else trPos.limit(negC.toInt).union(trNeg)
      training1.persist(MEMORY_AND_DISK)


      // rf
      println("working on rf ", i)
      createCrossValidator(sparkSession, training, randomForestModel(), "randomForest", i)
      val rfModel = randomForestModel().fit(training1)
      val rfResult = rfModel.transform(testing)
      //          evaluator.createConfusionMatrix(sparkSession, rfResult, "randomForest_outer", i)
      val rfResult1 = rfResult.select(rfResult("SN"), predUDF(rfResult("probability")) as ("randomForest"), rfResult("label"))
      //    rfResult.show()
      utility.appendParquetFile(sparkSession, rfResult1, "randomForest" + "Outer" + i)

      //nv
      println("working on nv ", i)

      createCrossValidator(sparkSession, training, naiveBayes(), "naivebayes", i)
      val nvModel = naiveBayes().fit(training1)
      val nvResult = nvModel.transform(testing)
      //          evaluator.createConfusionMatrix(sparkSession, nvResult, "naivebayes_outer", i)
      val nvResult1 = nvResult.select(nvResult("SN"), predUDF(nvResult("probability")) as ("naivebayes"), nvResult("label"))
      //    nvResult1.show()
      utility.appendParquetFile(sparkSession, nvResult1, "naivebayes" + "Outer" + i)


      //logistic
      println("working on lr ", i)

      createCrossValidator(sparkSession, training, logisticRegression(), "logistic", i)
      val lrModel = logisticRegression().fit(training1)
      val lrResult = lrModel.transform(testing)
      //          evaluator.createConfusionMatrix(sparkSession, lrResult, "logistic_outer", i)
      val lrResult1 = lrResult.select(lrResult("SN"), predUDF(lrResult("probability")) as ("logistic"), lrResult("label"))
      //    nvResult1.show()
      utility.appendParquetFile(sparkSession, lrResult1, "logistic" + "Outer" + i)

      //decisiontree
      println("working on dt ", i)

      createCrossValidator(sparkSession, training, decisionTree(), "decisiontree", i)
      val dtModel = decisionTree().fit(training1)
      val dtResult = dtModel.transform(testing)
      //      evaluator.createConfusionMatrix(sparkSession, dtResult, "dt_outer", i)
      val dtResult1 = dtResult.select(dtResult("SN"), predUDF(dtResult("probability")) as ("decisiontree"), dtResult("label"))
      //    nvResult1.show()
      utility.appendParquetFile(sparkSession, dtResult1, "decisiontree" + "Outer" + i)

      //
      //svm
      println("working on svm ", i)

      createCrossValidator(sparkSession, training, svmModel(), "svm", i)
      val svm = svmModel().fit(training1)
      val svmResult = svm.transform(testing)
      //      evaluator.createConfusionMatrix(sparkSession, svmResult, "svm_outer", i)
      val svmResult1 = svmResult.select(svmResult("SN"), svmProb(svmResult("rawPrediction")) as ("svm"), svmResult("label"))
      //    nvResult1.show()
      utility.appendParquetFile(sparkSession, svmResult1, "svm" + "Outer" + i)

      training.unpersist()
      testing.unpersist()
      training1.unpersist()
      //create outer classifier logistic regression

      //create testing
      val rfDFTes = utility.readParquetFile(sparkSession, "randomForest" + "Outer" + i)
      val lrDFTes = utility.readParquetFile(sparkSession, "logistic" + "Outer" + i)
      val svmDFTes = utility.readParquetFile(sparkSession, "svm" + "Outer" + i)
      val nvDFTes = utility.readParquetFile(sparkSession, "naivebayes" + "Outer" + i)
      val dtDFTes = utility.readParquetFile(sparkSession, "decisiontree" + "Outer" + i)

      val compDF = rfDFTes.join(lrDFTes.select("SN", "logistic"), "SN")
        .join(svmDFTes.select("SN", "svm"), "SN")
        .join(nvDFTes.select("SN", "naivebayes"), "SN")
        .join(dtDFTes.select("SN", "decisiontree"), "SN")

      compDF.persist(MEMORY_AND_DISK)
      val inputCols = compDF.columns.filter(_ != "label").filter(_ != "SN")


      import sparkSession.implicits._

      val testingCompositex = featureEngineering.featureAssembler(sparkSession, sparkSession.sparkContext, inputCols, "feature", compDF)
      val testingComposite = testingCompositex.select($"SN", $"features", $"label".cast("double") as ("label"))

      testingComposite.persist(MEMORY_AND_DISK)

      //create training
      val rfDFTr = utility.readParquetFile(sparkSession, "randomForest" + i)
      //      evaluator.createConfusionMatrix(sparkSession, rfDFTr, "randomForest", i)
      val rfDFTr1 = rfDFTr.groupBy(rfDFTr("SN"), rfDFTr("label")).mean("probability").withColumnRenamed("avg(probability)", "randomForest")

      val naiveDFTr = utility.readParquetFile(sparkSession, "naivebayes" + i)
      //      evaluator.createConfusionMatrix(sparkSession, naiveDFTr, "naivebayes", i)
      val naiveDFTr1 = naiveDFTr.groupBy(naiveDFTr("SN"), naiveDFTr("label")).mean("probability").withColumnRenamed("avg(probability)", "naivebayes")

      val lrDFTr = utility.readParquetFile(sparkSession, "logistic" + i)
      //      evaluator.createConfusionMatrix(sparkSession, lrDFTr, "logistic", i)
      val lrDFTr1 = lrDFTr.groupBy(lrDFTr("SN"), lrDFTr("label")).mean("probability").withColumnRenamed("avg(probability)", "logistic")

      val dtDFTr = utility.readParquetFile(sparkSession, "decisiontree" + i)
      //      evaluator.createConfusionMatrix(sparkSession, dtDFTr, "decisiontree", i)
      val dtDFTr1 = dtDFTr.groupBy(dtDFTr("SN"), dtDFTr("label")).mean("probability").withColumnRenamed("avg(probability)", "decisiontree")

      val svmDFTr = utility.readParquetFile(sparkSession, "svm" + i)
      //      evaluator.createConfusionMatrix(sparkSession, svmDFTr, "svm", i)
      val svmDFTr1 = svmDFTr.groupBy(svmDFTr("SN"), svmDFTr("label")).mean("probability").withColumnRenamed("avg(probability)", "svm")


      val compDFTr = rfDFTr1.join(lrDFTr1.select("SN", "logistic"), "SN")
        .join(naiveDFTr1.select("SN", "naivebayes"), "SN")
        .join(dtDFTr1.select("SN", "decisiontree"), "SN")
        .join(svmDFTr1.select("SN", "svm"), "SN")

      val trainingComposite = featureEngineering.featureAssembler(sparkSession, sparkSession.sparkContext, inputCols, "features", compDFTr).select($"SN", $"features", $"label".cast("double") as ("label"))

      trainingComposite.persist(MEMORY_AND_DISK)
      val outerLr = logisticRegression().fit(trainingComposite)
      val stackingLR = outerLr.transform(testingComposite)
      stackingLR.cache()

      val trainEvaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setMetricName("areaUnderROC")
      val auc = trainEvaluator.evaluate(stackingLR)
      //    utility.saveToCSV(sparkSession,stackingLR,"auc"+i,1)
      import org.apache.spark.sql.functions._
      val stackingLR1 = stackingLR.withColumn("iteration", lit(i))
      utility.appendParquetFile(sparkSession, stackingLR1, "stacking_all_prediction")
      //    utility.saveToCSV(sparkSession,stackingLR1,"stacking_all_prediction")
      import sparkSession.implicits._
      val aucDF = sparkSession.sparkContext.parallelize(Array((i, auc)))
      val audDF1 = aucDF.toDF("iteration", "aucValue")
      utility.appendParquetFile(sparkSession, audDF1, "auc")
      //      println("trainAUC: " + auc)
      //    dataWithFeature.count()
      //    dataWithFeature.show()
      //
      trainingComposite.unpersist()
      testingComposite.unpersist()
      compDF.unpersist()
      df.unpersist()
      snOuterDF.unpersist()

    }
    val allAucDF = utility.readParquetFile(sparkSession, "auc")
    utility.saveToCSV(sparkSession, allAucDF, "allAuc", 1)
    val stackingDF = utility.readParquetFile(sparkSession, "stacking_all_prediction")
    val trainEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")
    val aucTotal = trainEvaluator.evaluate(stackingDF)
    println(aucTotal, "aucTotal")
    import sparkSession.implicits._
    val aucAllDF = sparkSession.sparkContext.parallelize(Array((1, aucTotal)))
    val aucAllDF1 = aucAllDF.toDF("iteration", "aucValue")
    utility.saveToCSV(sparkSession, aucAllDF1, "totalAuc", 1)

  }

  def randomForestModel(): RandomForestClassifier = {
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    rf
  }

  def svmModel(): LinearSVC = {
    val lsvc = new LinearSVC()
      .setMaxIter(100)
      //      .setRegParam(0.1)
      .setTol(0.001)
    lsvc
  }

  def logisticRegression(): LogisticRegression = {

    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setProbabilityCol("probability")
    lr
  }

  def naiveBayes(): NaiveBayes = {
    val nv = new NaiveBayes()
    nv
  }

  def decisionTree(): DecisionTreeClassifier = {
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
    dt
  }

  def createCrossValidator[T](spark: SparkSession, df: DataFrame, model: T, modelType: String, outerIter: Int): Unit = {
    df.persist(MEMORY_AND_DISK)
    val snDF = df.select("SN")
    snDF.persist(MEMORY_AND_DISK)
    val snDF1: Array[DataFrame] = snDF.randomSplit(Array.fill[Double](10)(0.1))

    //     val outputDF:DataFrame=
    println("working on " + modelType + "  " + outerIter)

    modelType match {
      case "randomForest" => {
        val model1 = model.asInstanceOf[RandomForestClassifier]
        for (i <- (0 to 9).toList.par) {
          val snTr = snDF.except(snDF1(i))
          val training = df.join(snTr, "SN")
          training.cache()
          val testing = df.join(snDF1(i), "SN")

          //          training.show()
          val trPos = training.where("label=1")
          val trNeg = training.where("label=0")
          val posC = trPos.count()
          val negC = trNeg.count()
          val training1 = if (negC > posC) {
            trNeg.limit(posC.toInt).union(trPos)
          } else trPos.limit(negC.toInt).union(trNeg)
          training.unpersist()
          training1.persist(MEMORY_AND_DISK)
          testing.persist(MEMORY_AND_DISK)

          val calModel = model1.fit(training1)
          val result = calModel.transform(testing)
          //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
          val result1 = result.select(result("SN"), predUDF(result("probability")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))
          println(modelType)
          utility.appendParquetFile(spark, result1, modelType + outerIter)
          training1.unpersist()
          testing.unpersist()
        }

      }
      case "naivebayes" => {
        val model1 = model.asInstanceOf[NaiveBayes]
        for (i <- (0 to 9).toList.par) {
          val snTr = snDF.except(snDF1(i))
          val training = df.join(snTr, "SN")
          training.cache()
          val testing = df.join(snDF1(i), "SN")
          //          training.show()
          val trPos = training.where("label=1")
          val trNeg = training.where("label=0")
          val posC = trPos.count()
          val negC = trNeg.count()
          val training1 = if (negC > posC) {
            trNeg.limit(posC.toInt).union(trPos)
          } else trPos.limit(negC.toInt).union(trNeg)
          training.unpersist()
          training1.persist(MEMORY_AND_DISK)
          testing.persist(MEMORY_AND_DISK)

          val calModel = model1.fit(training1)
          val result = calModel.transform(testing)
          //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
          val result1 = result.select(result("SN"), predUDF(result("probability")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))
          utility.appendParquetFile(spark, result1, modelType + outerIter)
          training1.unpersist()
          testing.unpersist()
        }
      }
      case "logistic" => {
        val model1 = model.asInstanceOf[LogisticRegression]
        for (i <- (0 to 9).toList.par) {
          val snTr = snDF.except(snDF1(i))
          val training = df.join(snTr, "SN")
          val testing = df.join(snDF1(i), "SN")
          training.cache()
          //          training.show()
          val trPos = training.where("label=1")
          val trNeg = training.where("label=0")
          val posC = trPos.count()
          val negC = trNeg.count()
          val training1 = if (negC > posC) {
            trNeg.limit(posC.toInt).union(trPos)
          } else trPos.limit(negC.toInt).union(trNeg)
          training.unpersist()
          training1.persist(MEMORY_AND_DISK)
          testing.persist(MEMORY_AND_DISK)
          val calModel = model1.fit(training1)
          val result = calModel.transform(testing)
          //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
          val result1 = result.select(result("SN"), predUDF(result("probability")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))
          utility.appendParquetFile(spark, result1, modelType + outerIter)
          training1.unpersist()
          testing.unpersist()
        }

      }
      case "perceptron" => {
        val model1 = model.asInstanceOf[MultilayerPerceptronClassifier]
        for (i <- (0 to 9).toList.par) {
          val snTr = snDF.except(snDF1(i))
          val training = df.join(snTr, "SN")
          val testing = df.join(snDF1(i), "SN")
          //          training.show()
          val trPos = training.where("label=1")
          val trNeg = training.where("label=0")
          //          val posC = trPos.count()
          //          val negC = trNeg.count()
          //          val training1 = if (negC > posC) {
          //            trNeg.limit(posC.toInt).union(trPos)
          //          } else trPos.limit(negC.toInt).union(trNeg)

          training.show(false)
          val calModel = model1.fit(training)
          val result = calModel.transform(testing)
          //                    evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)

          val result1 = result.select(result("SN"), result("prediction") as ("probability"), result("label"), result("prediction"))
          utility.appendParquetFile(spark, result1, modelType + outerIter)
        }

      }

      case "svm" => {
        val model1 = model.asInstanceOf[LinearSVC]
        for (i <- (0 to 9).toList.par) {
          val snTr = snDF.except(snDF1(i))
          val training = df.join(snTr, "SN")
          training.cache()
          val testing = df.join(snDF1(i), "SN")

          //          training.show()
          val trPos = training.where("label=1")
          val trNeg = training.where("label=0")
          val posC = trPos.count()
          val negC = trNeg.count()
          val training1 = if (negC > posC) {
            trNeg.limit(posC.toInt).union(trPos)
          } else trPos.limit(negC.toInt).union(trNeg)
          training.unpersist()
          training1.persist(MEMORY_AND_DISK)
          testing.persist(MEMORY_AND_DISK)

          val calModel = model1.fit(training1)
          val result = calModel.transform(testing)
          //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
          val result1 = result.select(result("SN"), svmProb(result("rawPrediction")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))
          utility.appendParquetFile(spark, result1, modelType + outerIter)
          training1.unpersist()
          testing.unpersist()
        }

      }

      case "decisiontree" => {
        val model1 = model.asInstanceOf[DecisionTreeClassifier]
        for (i <- (0 to 9).toList.par) {
          println("dt", i)
          val snTr = snDF.except(snDF1(i))
          val training = df.join(snTr, "SN")
          training.cache()
          val testing = df.join(snDF1(i), "SN")
          //          training.show()
          val trPos = training.where("label=1")
          val trNeg = training.where("label=0")
          val posC = trPos.count()
          val negC = trNeg.count()
          val training1 = if (negC > posC) {
            trNeg.limit(posC.toInt).union(trPos)
          } else trPos.limit(negC.toInt).union(trNeg)
          training1.persist(MEMORY_AND_DISK)
          training.unpersist()
          val calModel = model1.fit(training1)
          val result = calModel.transform(testing)
          //          evaluator.createConfusionMatrix(spark, result, modelType, i, outerIter)
          val result1 = result.select(result("SN"), predUDF(result("probability")) as ("probability"), result("label"), result("rawPrediction"), result("prediction"))
          utility.appendParquetFile(spark, result1, modelType + outerIter)
          training1.unpersist()
        }

      }


      case _ => spark.emptyDataFrame

    }


    //    outputDF.show()
    df.unpersist()
    snDF.unpersist()
  }

  def calculateSimpleCrossValidator(sparkSession: SparkSession): Unit = {
    val data = service.getPIMAIndianData(sparkSession)

    //    modelEvaluator.createCrossValidator(data,"r")


    val inputCols = data.columns.filter(_ != "label").filter(_ != "SN")
    println(inputCols.mkString(","))
    import sparkSession.implicits._

    val dataWithFeaturex = featureEngineering.featureAssembler(sparkSession, sparkSession.sparkContext, inputCols, "features", data).select($"SN", $"features", $"label".cast("double") as ("label"))
    //
    val dataWithFeature = featureEngineering.scalar0_1(sparkSession, sparkSession.sparkContext, "features", "scaledFeature", dataWithFeaturex)
      .select($"SN", $"scaledFeature" as ("features"), $"label".cast("double") as ("label"))
    dataWithFeature.persist(MEMORY_AND_DISK)
    val snOuterDF = dataWithFeature.select("SN")
    //splitting for outer 10 cross validation
    val snOuterDF1: Array[DataFrame] = snOuterDF.randomSplit(Array.fill[Double](10)(0.1))
    //    for (i <- 0 to 9) {
    //      val snTr = snOuterDF.except(snOuterDF1(i))
    //      val training = scaledData.join(snTr, "SN")
    //      val testing = scaledData.join(snOuterDF1(i), "SN")
    //      //          training.show()
    //
    //      //for undersampling
    //      val trPos = training.where("label=1")
    //      val trNeg = training.where("label=0")
    //      val posC = trPos.count()
    //      val negC = trNeg.count()
    //      val training1 = if (negC > posC) {
    //        trNeg.limit(posC.toInt).union(trPos)
    //      } else trPos.limit(negC.toInt).union(trNeg)
    //
    //      println("iteration naive bayes", i)
    //      println(training1.where("label=1").count, "conunting in positive sample")
    //      println(training1.where("label=0").count, "conunting in negative sample")
    //
    //
    //      val nvModel = naiveBayes().fit(training1)
    //      val nvResult = nvModel.transform(testing)
    //      utility.appendParquetFile(sparkSession,nvResult,"nvResult")
    //      //    evaluator.createConfusionMatrix(sparkSession, nvResult, modelType, i)
    //
    //    }

    for (i <- 0 to 9) {
      val snTr = snOuterDF.except(snOuterDF1(i))
      val training = dataWithFeature.join(snTr, "SN")
      val testing = dataWithFeature.join(snOuterDF1(i), "SN")
      //          training.show()

      //for undersampling
      val trPos = training.where("label=1")
      val trNeg = training.where("label=0")
      val posC = trPos.count()
      val negC = trNeg.count()
      val training1 = if (negC > posC) {
        trNeg.limit(posC.toInt).union(trPos)
      } else trPos.limit(negC.toInt).union(trNeg)

      println("iteration lr", i)
      println(training1.where("label=1").count, "conunting in positive sample")
      println(training1.where("label=0").count, "conunting in negative sample")
      println(training.count, "conunting in negative sample")


      //      val lrModel = logisticRegression().fit(training1)
      //      val lrResult = lrModel.transform(testing)
      //      utility.appendParquetFile(sparkSession,lrResult,"lrResult")

      val nvModel = naiveBayes().fit(training1)
      val nvResult = nvModel.transform(testing)
      //      utility.appendParquetFile(sparkSession,nvResult,"nvResult")
      evaluator.createConfusionMatrix(sparkSession, nvResult, "nv", i)

    }


  }

  def calculateROC(sparkSession: SparkSession): Unit = {
    val nvResult1 = utility.readParquetFile(sparkSession, "nvResult")
    nvResult1.show()
    val trainEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")
    val aucTotal = trainEvaluator.evaluate(nvResult1)
    println("total value of AUC curve is nv", aucTotal)


    val lrResult1 = utility.readParquetFile(sparkSession, "lrResult")
    lrResult1.show()

    val aucTotal1 = trainEvaluator.evaluate(lrResult1)
    println("total value of AUC curve is lr", aucTotal1)

    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")


    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    println("Test set presision lr = " + evaluator1.evaluate(lrResult1))
    println("Test set presision nv = " + evaluator1.evaluate(nvResult1))

    println("Test set recall lr = " + evaluator2.evaluate(lrResult1))
    println("Test set recall nv = " + evaluator2.evaluate(nvResult1))


  }

  def calculateSVMProbability(v: org.apache.spark.ml.linalg.Vector): Double = {
    val distance = v(1)
    val probability = 1 / (1 + scala.math.exp(-distance))
    probability
  }

  def combination(sparkSession: SparkSession, i: Int, dir: String, data_name: String): DataFrame = {

    //create testing
    import sparkSession.implicits._

    val data = utility.readCSV(sparkSession, data_name).select("SN", "label").cache()
    val rfDFTes = utility.readParquetFile(sparkSession, dir + "/" + "randomForest" + "outer" + i)
    val lrDFTes = utility.readParquetFile(sparkSession, dir + "/" + "logistic" + "outer" + i)
    val svmDFTes = utility.readParquetFile(sparkSession, dir + "/" + "svm" + "outer" + i)
    val nvDFTes = utility.readParquetFile(sparkSession, dir + "/" + "naivebayes" + "outer" + i)
    val dtDFTes = utility.readParquetFile(sparkSession, dir + "/" + "decisiontree" + "outer" + i)

    val compDF = rfDFTes.join(lrDFTes.select("SN", "logistic"), "SN")
      .join(svmDFTes.select("SN", "svm"), "SN")
      .join(nvDFTes.select("SN", "naivebayes"), "SN")
      .join(dtDFTes.select("SN", "decisiontree"), "SN")
      .join(data, "SN")

    compDF.persist(MEMORY_AND_DISK)
    val inputCols = compDF.columns.filter(_ != "label").filter(_ != "SN")
    val testingCompositex = featureEngineering.featureAssembler(sparkSession, sparkSession.sparkContext, inputCols, "feature", compDF)
    val testingComposite = testingCompositex.select($"SN", $"features", $"label".cast("double") as ("label"))

    testingComposite.persist(MEMORY_AND_DISK)


    //create training
    val rfDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, dir + "/" + "Result/" + i + "randomForest" + j)

    }).reduce((a, b) => a.unionAll(b))


    //    val rfDFTr = utility.readParquetFile(sparkSession, "randomForest" + i)

    val naiveDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, dir + "/" + "Result/" + i + "naivebayes" + j)

    }).reduce((a, b) => a.unionAll(b))

    //      utility.readParquetFile(sparkSession, "naivebayes" + i)

    val lrDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, dir + "/" + "Result/" + i + "logistic" + j)

    }).reduce((a, b) => a.unionAll(b))

    //      utility.readParquetFile(sparkSession, "logistic" + i)

    val dtDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, dir + "/" + "Result/" + i + "decisiontree" + j)

    }).reduce((a, b) => a.unionAll(b))
    //      utility.readParquetFile(sparkSession, "decisiontree" + i)

    val svmDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, dir + "/" + "Result/" + i + "svm" + j)

    }).reduce((a, b) => a.unionAll(b))
    //      utility.readParquetFile(sparkSession, "svm" + i)

    val compDFTr = rfDFTr.join(lrDFTr.select("SN", "logistic"), "SN")
      .join(naiveDFTr.select("SN", "naivebayes"), "SN")
      .join(dtDFTr.select("SN", "decisiontree"), "SN")
      .join(svmDFTr.select("SN", "svm"), "SN")
      .join(data, "SN")

    val trainingComposite = featureEngineering.featureAssembler(sparkSession, sparkSession.sparkContext, inputCols, "features", compDFTr).select($"SN", $"features", $"label".cast("double") as ("label"))
    //
    trainingComposite.persist(MEMORY_AND_DISK)
    val outerLrModel = logisticRegression().fit(trainingComposite)
    val stackingResult = outerLrModel.transform(testingComposite)
    stackingResult


  }

  def evaluationBaseClassifier(sparkSession: SparkSession, i: Int): Unit = {

    //create testing

    val rfDFTes = utility.readParquetFile(sparkSession, "randomForest" + "outer" + i)
    val lrDFTes = utility.readParquetFile(sparkSession, "logistic" + "outer" + i)
    val svmDFTes = utility.readParquetFile(sparkSession, "svm" + "outer" + i)
    val nvDFTes = utility.readParquetFile(sparkSession, "naivebayes" + "outer" + i)
    val dtDFTes = utility.readParquetFile(sparkSession, "decisiontree" + "outer" + i)

    evaluator.createConfusionMatrix(sparkSession, rfDFTes, "rfOuter", 0, i)
    evaluator.createConfusionMatrix(sparkSession, lrDFTes, "lrOuter", 0, i)
    evaluator.createConfusionMatrix(sparkSession, svmDFTes, "svmOuter", 0, i)
    evaluator.createConfusionMatrix(sparkSession, nvDFTes, "nvOuter", 0, i)
    evaluator.createConfusionMatrix(sparkSession, dtDFTes, "dtOuter", 0, i)


    //create training
    val rfDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "Result/" + i + "randomForest" + j)

    }).reduce((a, b) => a.unionAll(b))


    //    val rfDFTr = utility.readParquetFile(sparkSession, "randomForest" + i)

    val naiveDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "Result/" + i + "naivebayes" + j)

    }).reduce((a, b) => a.unionAll(b))

    //      utility.readParquetFile(sparkSession, "naivebayes" + i)

    val lrDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "Result/" + i + "logistic" + j)

    }).reduce((a, b) => a.unionAll(b))

    //      utility.readParquetFile(sparkSession, "logistic" + i)

    val dtDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "Result/" + i + "decisiontree" + j)

    }).reduce((a, b) => a.unionAll(b))
    //      utility.readParquetFile(sparkSession, "decisiontree" + i)

    val svmDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "Result/" + i + "svm" + j)

    }).reduce((a, b) => a.unionAll(b))
    //      utility.readParquetFile(sparkSession, "svm" + i)

    evaluator.createConfusionMatrix(sparkSession, rfDFTr, "rfInner", 0, i)
    evaluator.createConfusionMatrix(sparkSession, lrDFTr, "lrInner", 0, i)
    evaluator.createConfusionMatrix(sparkSession, svmDFTr, "svmInner", 0, i)
    evaluator.createConfusionMatrix(sparkSession, naiveDFTr, "nvInner", 0, i)
    evaluator.createConfusionMatrix(sparkSession, dtDFTr, "dtInner", 0, i)


  }

  def evaluationBaseClassifierOuter(sparkSession: SparkSession, i: Int): Unit = {

    //create training
    val rfDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "randomForest" + "outer" + j)
    }).reduce((a, b) => a.unionAll(b))


    //    val rfDFTr = utility.readParquetFile(sparkSession, "randomForest" + i)

    val naiveDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "naivebayes" + "outer" + j)

    }).reduce((a, b) => a.unionAll(b))

    //      utility.readParquetFile(sparkSession, "naivebayes" + i)

    val lrDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "logistic" + "outer" + j)

    }).reduce((a, b) => a.unionAll(b))

    //      utility.readParquetFile(sparkSession, "logistic" + i)

    val dtDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "decisiontree" + "outer" + j)

    }).reduce((a, b) => a.unionAll(b))
    //      utility.readParquetFile(sparkSession, "decisiontree" + i)

    val svmDFTr = (for (j <- 0 to 4) yield {
      utility.readParquetFile(sparkSession, "svm" + "outer" + j)

    }).reduce((a, b) => a.unionAll(b))
    //      utility.readParquetFile(sparkSession, "svm" + i)

    //    evaluator.createConfusionMatrix(sparkSession,rfDFTr, "rf",0,i)
    //    evaluator.createConfusionMatrix(sparkSession,lrDFTr, "lr",0,i)
    //    evaluator.createConfusionMatrix(sparkSession,svmDFTr, "svm",0,i)
    //    evaluator.createConfusionMatrix(sparkSession,naiveDFTr, "nv",0,i)
    //    evaluator.createConfusionMatrix(sparkSession,dtDFTr, "dt",0,i)
    utility.saveParquetFile(sparkSession, rfDFTr, "rfDFTr")
    utility.saveParquetFile(sparkSession, naiveDFTr, "naiveDFTr")
    utility.saveParquetFile(sparkSession, lrDFTr, "lrDFTr")
    utility.saveParquetFile(sparkSession, svmDFTr, "svmDFTr")
    utility.saveParquetFile(sparkSession, dtDFTr, "dtDFTr")


  }


}
