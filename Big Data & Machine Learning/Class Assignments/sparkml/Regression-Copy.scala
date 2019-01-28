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
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Imputer
case class ColumnInfo(col: Int, name: String, len: Int);

object RegressionCopy{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RegressionCopy").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    print("!!!!!!!!!!!\n"*10)
    val colInfo = spark.read.
                  schema(Encoders.product[ColumnInfo].schema).
                  option("delimiter", "\t").
                  csv("/data/BigData/brfss/Columns.txt").
                  as[ColumnInfo]

    val colNames = colInfo.filter($"name".isNotNull).map(_.name).collect()
    var cnt = -1
    val dataSchema = StructType(Array.fill(colNames.size){cnt+=1
                                                          StructField(colNames(cnt), DoubleType, true)
    })
    val colAndLen = colInfo.filter($"col".isNotNull && $"len".isNotNull).map(c => (c.col, c.len)).collect()
    def parseData(s: String) : Row = {
      var x = 0
      var seq:Seq[Any] = Seq.fill(colAndLen.size)(null)
      for(x <- 0 until colAndLen.size)
      {
        val temp = s.substring(colAndLen(x)._1 - 1, (colAndLen(x)._1-1) + colAndLen(x)._2)
        if(!temp.contains(" "))
           seq(x) = temp.toDouble
      }
      Row.fromSeq(seq)
    }
    val rddData = spark.sparkContext.textFile("/data/BigData/brfss/LLCP2016.asc").
               map(parseData(_))
    val data = spark.createDataFrame(rddData, dataSchema)
    // data.withColumn("GENHLTH", when($"GENHLTH" === 7 || $"GENHLTH" === 9, 0.0).otherwise($"GENHLTH")).
    //      withColumn("PHYSHLTH", when($"PHYSHLTH" === 77 || $"PHYSHLTH" === 88 || $"PHYSHLTH" === 99, 0).otherwise($"PHYSHLTH")).
    //      withColumn("MENTHLTH", when($"MENTHLTH" === 77 || $"MENTHLTH" === 88 || $"MENTHLTH" === 99, 0).otherwise($"MENTHLTH")).
    //      withColumn("POORHLTH", when($"POORHLTH" === 77 || $"POORHLTH" === 88 || $"POORHLTH" === 99, 0).otherwise($"POORHLTH")).
    //      withColumn("SLEPTIM1", when($"SLEPTIM1" === 77 || $"SLEPTIM1" === 88 || $"SLEPTIM1" === 99, 0).otherwise($"SLEPTIM1")).
    //      withColumn("EXERANY2", when($"EXERANY2" === 7 || $"EXERANY2" === 9 |, 0).otherwise($"EXERANY2"))

    val imputer = new Imputer()
        .setInputCols(data.columns)
        .setOutputCols(data.columns.map(c => s"${c}_imputed"))
        .setStrategy("mean")

    val imputedData = imputer.fit(data).transform(data).cache()
    val usableFeaturesGH = data.columns.filter(_ != "GENHLTH").map(c => s"${c}_imputed")
    val ghFtrsVA = new VectorAssembler().setInputCols(usableFeaturesGH).setOutputCol("ghFtrs")
    val ghWithVector = ghFtrsVA.transform(imputedData)
    val Array(ghTrain, ghTest) = ghWithVector.randomSplit(Array(0.8, 0.2))
    val ghGBT = new GBTRegressor().setFeaturesCol("ghFtrs").setLabelCol("GENHLTH").setMaxIter(10)
    val ghGBTModel = ghGBT.fit(ghTrain)
    val ghGBTPred = ghGBTModel.transform(ghTest)
    ghGBTPred.select($"prediction", $"GENHLTH", $"ghFtrs").show()
    val ghGBTEval = new RegressionEvaluator().setLabelCol("GENHLTH").setPredictionCol("prediction").
                    setMetricName("rmse")
    println(ghGBTEval.evaluate(ghGBTPred))
    println(ghGBTModel.featureImportances)

    // val ghFtrsVA = new VectorAssembler().setInputCols(usableFeaturesGH).setOutputCol("ghFtrs")
    // val ghWithVector = ghFtrsVA.transform(data)
    // val Array(ghTrain, ghTest) = ghWithVector.randomSplit(Array(0.8, 0.2))
    // val ghLR = new LinearRegression().setMaxIter(10).setFeaturesCol("ghFtrs")
    //   .setLabelCol("GENHLTH")
    // val ghLRModel = ghLR.fit(ghTrain)
    // println(ghLRModel.coefficients, ghLRModel.intercept)
    // println(ghLRModel.summary.r2)
    // data.select($"GENHLTH").filter($"GENHLTH" =!= 0.0).describe().show()
    //
    // val ghLRPred = ghLRModel.transform(ghTest)
    // ghLRPred.select($"ghFtrs", $"GENHLTH", $"prediction").show(5)
    // val ghLREvaluator = new RegressionEvaluator().setPredictionCol("prediction").
    //                 setLabelCol("GENHLTH").setMetricName("r2")
    // println(ghLREvaluator.evaluate(ghLRPred))
    // val ghTestResult = ghLRModel.evaluate(ghTest)
    // println(ghTestResult.rootMeanSquaredError)

    println("!!!!!!!!!!!\n"*10)
    data.unpersist()
    spark.stop()
  }
}
//--master spark://pandora00:7077
