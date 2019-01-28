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
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation


case class ColumnInfo(col: Int, name: String, len: Int);

object Regression {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("LocalAreaUnemployment").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    println("!!!!!!!!!\n"*10)
    val colInfo = spark.read.
                  schema(Encoders.product[ColumnInfo].schema).
                  option("delimiter", "\t").
                  csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparkml/data/brfss/Columns.txt").
                  as[ColumnInfo]

    val colNames = colInfo.map(_.name).collect()
    var cnt = -1
    val dataSchema = StructType(Array.fill(colNames.size){cnt+=1
                                                          StructField(colNames(cnt), DoubleType, true)
    })
    val colAndLen = colInfo.map(c => (c.col, c.len)).collect()
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
    val rddData = spark.sparkContext.textFile("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparkml/data/brfss/LLCP2016.asc").
               map(parseData(_))
    val data = spark.createDataFrame(rddData, dataSchema).
         withColumn("GENHLTH", when($"GENHLTH" === 7 || $"GENHLTH" === 9, 0.0).otherwise($"GENHLTH")).
         withColumn("PHYSHLTH", when($"PHYSHLTH" === 77 || $"PHYSHLTH" === 88 || $"PHYSHLTH" === 99, 0).otherwise($"PHYSHLTH")).
         withColumn("MENTHLTH", when($"MENTHLTH" === 77 || $"MENTHLTH" === 88 || $"MENTHLTH" === 99, 0).otherwise($"MENTHLTH")).
         withColumn("POORHLTH", when($"POORHLTH" === 77 || $"POORHLTH" === 88 || $"POORHLTH" === 99, 0).otherwise($"POORHLTH")).
         withColumn("SLEPTIM1", when($"SLEPTIM1" === 77 || $"SLEPTIM1" === 88 || $"SLEPTIM1" === 99, 0).otherwise($"SLEPTIM1")).
         withColumn("EXERANY2", when($"EXERANY2" === 7 || $"EXERANY2" === 9, 0).otherwise($"EXERANY2"))

    val imputer = new Imputer()
        .setInputCols(data.columns)
        .setOutputCols(data.columns.map(c => s"${c}_imputed"))
        .setStrategy("mean")

    val imputedData = imputer.fit(data).transform(data)
    //val usableFeaturesGH = data.columns.filter(_ != "GENHLTH").map(c => s"${c}_imputed")
    val usableFeaturesGH = data.columns.filter(c => c != "GENHLTH" && c != "PHYSHLTH" && c != "MENTHLTH" && c != "POORHLTH").map(c => s"${c}_imputed")
    val ghFtrsVA = new VectorAssembler().setInputCols(usableFeaturesGH).setOutputCol("ghFtrs")
    val ghWithVector = ghFtrsVA.transform(imputedData)
    val Row(coeff1: Matrix) = Correlation.corr(ghWithVector, "ghFtrs").head()
    println(coeff1.toString)
    val Array(ghTrain, ghTest) = ghWithVector.randomSplit(Array(0.8, 0.2))
    val ghGBT = new DecisionTreeRegressor().setFeaturesCol("ghFtrs").setLabelCol("GENHLTH_imputed")
    val ghGBTModel = ghGBT.fit(ghTrain)
    val ghGBTPred = ghGBTModel.transform(ghTest)
    ghGBTPred.select($"prediction", $"GENHLTH", $"ghFtrs").show()
    val ghGBTEval = new RegressionEvaluator().setLabelCol("GENHLTH_imputed").setPredictionCol("prediction").
                    setMetricName("rmse")
    println(ghGBTEval.evaluate(ghGBTPred))
    val ghGBTEval1 = new RegressionEvaluator().setLabelCol("GENHLTH_imputed").setPredictionCol("prediction").
                    setMetricName("r2")
    println(ghGBTEval1.evaluate(ghGBTPred))
    // var importanceVec = ghGBTModel.featureImportances
    // var mostImportant = importanceVec.toArray
    // var tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesGH(tIndex) + ": " + mostImportant(tIndex))
    // mostImportant(tIndex) = 0
    // tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesGH(tIndex) + ": " + mostImportant(tIndex))
    // mostImportant(tIndex) = 0
    // tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesGH(tIndex) + ": " + mostImportant(tIndex))
    // 2
    // 0.6602112299997196
    // 0.6354186512526242
    // _RFHLTH_imputed: 0.9187424994234894
    // _PHYS14D_imputed: 0.029551132470825074
    // _BMI5_imputed: 0.02106129031926715

    // 3
    // 0.6583638191222357
    // 0.6359021734887449
    // _RFHLTH_imputed: 0.9172723372765326
    // _PHYS14D_imputed: 0.022370407697924068
    // _BMI5_imputed: 0.021646960554928922


    // //val usableFeaturesPH = data.columns.filter(_ != "PHYSHLTH").map(c => s"${c}_imputed")
    // val usableFeaturesPH = data.columns.filter(c => c != "GENHLTH" && c != "PHYSHLTH" && c != "MENTHLTH" && c != "POORHLTH").map(c => s"${c}_imputed")
    // val phFtrsVA = new VectorAssembler().setInputCols(usableFeaturesPH).setOutputCol("phFtrs")
    // val phWithVector = phFtrsVA.transform(imputedData)
    // val Array(phTrain, phTest) = phWithVector.randomSplit(Array(0.8, 0.2))
    // val phDT = new DecisionTreeRegressor().setFeaturesCol("phFtrs").setLabelCol("PHYSHLTH_imputed")
    // val phDTModel = phDT.fit(phTrain)
    // val phDTPred = phDTModel.transform(phTest)
    // phDTPred.select($"prediction", $"PHYSHLTH", $"phFtrs").show()
    // val phDTEval = new RegressionEvaluator().setLabelCol("PHYSHLTH_imputed").setPredictionCol("prediction").
    //                 setMetricName("rmse")
    // println(phDTEval.evaluate(phDTPred))
    // val phDTEval1 = new RegressionEvaluator().setLabelCol("PHYSHLTH_imputed").setPredictionCol("prediction").
    //                 setMetricName("r2")
    // println(phDTEval1.evaluate(phDTPred))
    // val importanceVec = phDTModel.featureImportances
    // var mostImportant = importanceVec.toArray
    // var tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesPH(tIndex) + ": " + mostImportant(tIndex))
    // mostImportant(tIndex) = 0
    // tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesPH(tIndex) + ": " + mostImportant(tIndex))
    // mostImportant(tIndex) = 0
    // tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesPH(tIndex) + ": " + mostImportant(tIndex))

    //2
    // 2.6397873325232335
    // 0.9107686350673976
    // _PHYS14D_imputed: 0.9924408884855432
    // _RFHLTH_imputed: 0.005034006057649851
    // DIFFWALK_imputed: 0.0013423307066426579
    // 3
    // 2.493391353274906
    // 0.9207212319621405
    // _PHYS14D_imputed: 0.9825047296043835
    // POORHLTH_imputed: 0.013126227283352259
    // GENHLTH_imputed: 0.0032928108662473978


    //val usableFeaturesMH = data.columns.filter(_ != "MENTHLTH").map(c => s"${c}_imputed")
    // val usableFeaturesMH = data.columns.filter(c => c != "GENHLTH" && c != "PHYSHLTH" && c != "MENTHLTH" && c != "POORHLTH").map(c => s"${c}_imputed")
    // val mhFtrsVA = new VectorAssembler().setInputCols(usableFeaturesMH).setOutputCol("mhFtrs")
    // val mhWithVector = mhFtrsVA.transform(imputedData)
    // val Array(mhTrain, mhTest) = mhWithVector.randomSplit(Array(0.8, 0.2))
    // val mhDT = new DecisionTreeRegressor().setFeaturesCol("mhFtrs").setLabelCol("MENTHLTH_imputed")
    // val mhDTModel = mhDT.fit(mhTrain)
    // val mhDTPred = mhDTModel.transform(mhTest)
    // mhDTPred.select($"prediction", $"MENTHLTH", $"mhFtrs").show()
    // val mhDTEval = new RegressionEvaluator().setLabelCol("MENTHLTH_imputed").setPredictionCol("prediction").
    //                 setMetricName("rmse")
    // println(mhDTEval.evaluate(mhDTPred))
    // val mhDTEval1 = new RegressionEvaluator().setLabelCol("MENTHLTH_imputed").setPredictionCol("prediction").
    //                 setMetricName("r2")
    // println(mhDTEval1.evaluate(mhDTPred))
    // val importanceVec = mhDTModel.featureImportances
    // var mostImportant = importanceVec.toArray
    // var tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesMH(tIndex) + ": " + mostImportant(tIndex))
    // mostImportant(tIndex) = 0
    // tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesMH(tIndex) + ": " + mostImportant(tIndex))
    // mostImportant(tIndex) = 0
    // tIndex = mostImportant.indices.maxBy(mostImportant)
    // println(usableFeaturesMH(tIndex) + ": " + mostImportant(tIndex))

  // 2
  // 2.4586716588782656
  // 0.8984797550162852
  // _MENT14D_imputed: 0.9941238262609267
  // _PHYS14D_imputed: 0.0026699565230578147
  // ADDEPEV2_imputed: 0.0013418096390195645
  // 3
  // 2.3898045858264445
  // 0.9043899189650789
  // _MENT14D_imputed: 0.98941029386919
  // POORHLTH_imputed: 0.008194574726652332
  // PHYSHLTH_imputed: 0.0015920810854303986

  //val usableFeaturesPRH = data.columns.filter(_ != "POORHLTH").map(c => s"${c}_imputed")
  val usableFeaturesPRH = data.columns.filter(c => c != "GENHLTH" && c != "PHYSHLTH" && c != "MENTHLTH" && c != "POORHLTH").map(c => s"${c}_imputed")
  val prhFtrsVA = new VectorAssembler().setInputCols(usableFeaturesPRH).setOutputCol("prhFtrs")
  val prhWithVector = prhFtrsVA.transform(imputedData)
  val Array(prhTrain, prhTest) = prhWithVector.randomSplit(Array(0.8, 0.2))
  val prhDT = new DecisionTreeRegressor().setFeaturesCol("prhFtrs").setLabelCol("POORHLTH_imputed")
  val prhDTModel = prhDT.fit(prhTrain)
  val prhDTPred = prhDTModel.transform(prhTest)
  prhDTPred.select($"prediction", $"POORHLTH_imputed", $"prhFtrs").show()
  val prhDTEval = new RegressionEvaluator().setLabelCol("POORHLTH_imputed").setPredictionCol("prediction").
                  setMetricName("rmse")
  println(prhDTEval.evaluate(prhDTPred))
  val prhDTEval1 = new RegressionEvaluator().setLabelCol("POORHLTH_imputed").setPredictionCol("prediction").
                  setMetricName("r2")
  println(prhDTEval1.evaluate(prhDTPred))
  val importanceVec = prhDTModel.featureImportances
  var mostImportant = importanceVec.toArray
  var tIndex = mostImportant.indices.maxBy(mostImportant)
  println(usableFeaturesPRH(tIndex) + ": " + mostImportant(tIndex))
  mostImportant(tIndex) = 0
  tIndex = mostImportant.indices.maxBy(mostImportant)
  println(usableFeaturesPRH(tIndex) + ": " + mostImportant(tIndex))
  mostImportant(tIndex) = 0
  tIndex = mostImportant.indices.maxBy(mostImportant)
  println(usableFeaturesPRH(tIndex) + ": " + mostImportant(tIndex))
  // 2
  // 5.340036878601146
  // 0.38317552203880256
  // _PHYS14D_imputed: 0.6362436686547875
  // DIFFALON_imputed: 0.18557321845433888
  // _MENT14D_imputed: 0.09375085268004728

  // 3
  // 5.191058274457757
  // 0.40746737641778263
  // PHYSHLTH_imputed: 0.6143575600047829
  // DIFFALON_imputed: 0.13501821495715902
  // MENTHLTH_imputed: 0.08779748612104167

    println("!!!!!!!!!!!\n"*10)
    data.unpersist()
    spark.stop()
  }
}
