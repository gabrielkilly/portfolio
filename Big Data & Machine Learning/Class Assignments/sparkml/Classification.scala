package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql._
import org.apache.spark.ml.linalg
import scala.collection.mutable._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, BinaryClassificationEvaluator}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.ProbabilisticClassifier
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LinearSVC

object Classification {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkClassification").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("\n!!!!!!!!" * 6)

    val dataNames = (0 until 47).toArray.map("_c"+_.toString)
    var cnt = -1
    val admissionDataSchema = StructType(Array.fill(dataNames.size){cnt+=1
                                                          if(cnt != 5 && cnt != 6 && cnt != 7)
                                                            StructField(dataNames(cnt), DoubleType)
                                                          else
                                                            StructField(dataNames(cnt), StringType)
    })

    val admissionData = spark.read.schema(admissionDataSchema)
                        .option("header", "true")
                        .option("delimiter", "\t")
                        .csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparkml/data/classification/AdmissionAnon.tsv")
    //1. How many rows and columns does the data have?
    val rows = admissionData.count()
    val columns = admissionData.columns.length
    println("Rows: " + rows + ", Columns: " + columns)

    //2. How many different values does the last column have?
    val lastColName = admissionData.columns(columns - 1)
    val differentVals = admissionData.select(admissionData(lastColName)).distinct()
    differentVals.show()

    //3. How many rows are there with each of those valuescoeff1?
    val lastColValueCounts = admissionData.groupBy(admissionData(lastColName))
                                          .agg(count(lastColName))
    lastColValueCounts.show()

    //4. Using the corr method of the org.apache.spark.ml.stat.Correlation object, calculate the correlation matrix for the numeric columns.
    //println(admissionData.printSchema()) 5,6,7
    val numericColumns = admissionData.columns.filter(c => c != "_c5" && c != "_c6" && c != "_c7")
    val imputer = new Imputer()
        .setInputCols(numericColumns)
        .setOutputCols(numericColumns.map(c => s"${c}_imputed"))
        .setStrategy("mean")
    val imputedData = imputer.fit(admissionData).transform(admissionData)
    val imputedColNames = numericColumns.map(c => s"${c}_imputed")
    var numericDataVA = new VectorAssembler().setInputCols(imputedColNames).setOutputCol("ndFtrs")
    val adva = numericDataVA.transform(imputedData)
    val Row(coeff1: Matrix) = Correlation.corr(adva, "ndFtrs").head()
    println(coeff1.toString())

    val matrix = spark.sparkContext.parallelize(coeff1.rowIter.toSeq).map(_.toArray)
    val lastIndex = matrix.take(1)(0).length - 1
    var currIndex = 0
    var corrToLastColumn = matrix.collect().map{ arr =>
        currIndex += 1
        (Math.abs(arr(lastIndex)), currIndex)
    }.sortBy(_._1)(Ordering[Double].reverse)
    corrToLastColumn.foreach(println)

    //6a
    val mcLabelCol = "_c46_imputed"
    val mcColNames = imputedColNames.filter(_ != mcLabelCol)
    val multiClassifyVec = new VectorAssembler().setInputCols(mcColNames).setOutputCol("mcFtrs")
    val mcData = multiClassifyVec.transform(imputedData)
    val mcClassifier = new RandomForestClassifier().setFeaturesCol("mcFtrs").setLabelCol(mcLabelCol)
    val Array(mcTrain, mcTest) = mcData.randomSplit(Array(0.8, 0.2))
    val mcModel = mcClassifier.fit(mcTrain)
    val mcPred = mcModel.transform(mcTest)
    mcPred.select($"prediction", mcPred(mcLabelCol), $"mcFtrs").show()
    val mcEval = new MulticlassClassificationEvaluator().setLabelCol(mcLabelCol).setPredictionCol("prediction")
    println("F1: " + mcEval.evaluate(mcPred))
    val mcFtrImportance = mcModel.featureImportances.toArray
                         .zip(mcColNames)
                         .sortBy(_._1)(Ordering[Double].reverse)
    println("\nFeature Importance\n")
    mcFtrImportance.foreach(println)

    //6b
    val bcd = imputedData.withColumn(mcLabelCol, when(imputedData(mcLabelCol) === 0 || imputedData(mcLabelCol) === 1, 0).otherwise(1))
    val bcColNames = imputedColNames.filter(_ != mcLabelCol)
    val binaryClassifyVec = new VectorAssembler().setInputCols(bcColNames).setOutputCol("bcFtrs")
    val bcData = binaryClassifyVec.transform(bcd)
    val bcClassifier = new GBTClassifier().setFeaturesCol("bcFtrs").setLabelCol(mcLabelCol)
    val Array(bcTrain, bcTest) = bcData.randomSplit(Array(0.8, 0.2))
    val bcModel = bcClassifier.fit(bcTrain)
    val bcPred = bcModel.transform(bcTest)
    bcPred.select($"prediction", bcPred(mcLabelCol), $"bcFtrs").show()
    val bcEval = new BinaryClassificationEvaluator().setLabelCol(mcLabelCol).setRawPredictionCol("prediction")
    println("F1: " + bcEval.evaluate(bcPred))
    val bcFtrImportance = bcModel.featureImportances.toArray
                         .zip(bcColNames)
                         .sortBy(_._1)(Ordering[Double].reverse)
    println("\nFeature Importance\n")
    bcFtrImportance.foreach(println)

    //DecisionTree
    //F1: 0.5045265570265804
    //F1: 0.7188093622795116

    //GBT
    //// XXX:
    //0.7040856838647971
    // (0.0712176208319405,_c22_imputed)
    // (0.05065134635917524,_c9_imputed)
    // (0.049726300383036735,_c23_imputed)
    // (0.04721151716592172,_c0_imputed)
    // (0.04574310061183844,_c21_imputed)
    // (0.044419002577192564,_c4_imputed)
    // (0.04162866859752465,_c26_imputed)
    // (0.03911201543126565,_c19_imputed)
    // (0.03805230242837745,_c11_imputed)
    // (0.03564162081285036,_c42_imputed)

    //RandomForest
    //F1: 0.5262038783576273
    // (0.2500864122315225,_c22_imputed)
    // (0.0952608385023867,_c9_imputed)
    // (0.08799196503952879,_c12_imputed)
    // (0.06635942689227789,_c23_imputed)
    // (0.06181445619690357,_c11_imputed)
    // (0.029542933881774265,_c45_imputed)
    // (0.0289969644579821,_c13_imputed)
    // (0.02702014927091087,_c42_imputed)
    // (0.021810369913079487,_c44_imputed)
    //F1: 0.5778226911482598
    //(0.36132189756878086,_c22_imputed)
    // (0.14333203552526633,_c23_imputed)
    // (0.10310421026873648,_c9_imputed)
    // (0.025878497927431576,_c13_imputed)
    // (0.02510396397762823,_c45_imputed)
    // (0.021889957882170127,_c39_imputed)
    // (0.021774032668859335,_c42_imputed)
    // (0.018278018195772478,_c44_imputed)
    // (0.01610544779482859,_c0_imputed)
    // (0.015582130810434188,_c12_imputed)

    // LogisticRegression
    // F1: 0.5225344831403146
    // F1: 0.7040114804431985

    // LVC
    //// XXX:
    // 0.6415845648604269







    println("\n!!!!!!!!" * 6)
    spark.stop()
  }
}
