package finalproject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql._
import org.apache.spark.ml.linalg
import scala.collection.mutable._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._


object HurricaneAnalysis extends App{
//  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("HurricaneAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    println("\n!!!!!!!!" * 6)

    // val columnNames = "STATION,STATION_NAME,ELEVATION,LATITUDE,LONGITUDE,DATE,REPORTTPYE,HOURLYSKYCONDITIONS,HOURLYVISIBILITY,HOURLYPRSENTWEATHERTYPE,HOURLYDRYBULBTEMPF,HOURLYDRYBULBTEMPC,HOURLYWETBULBTEMPF,HOURLYWETBULBTEMPC,HOURLYDewPointTempF,HOURLYDewPointTempC,HOURLYRelativeHumidity,HOURLYWindSpeed,HOURLYWindDirection,HOURLYWindGustSpeed,HOURLYStationPressure,HOURLYPressureTendency,HOURLYPressureChange,HOURLYSeaLevelPressure,HOURLYPrecip,HOURLYAltimeterSetting,DAILYMaximumDryBulbTemp,DAILYMinimumDryBulbTemp,DAILYAverageDryBulbTemp,DAILYDeptFromNormalAverageTemp,DAILYAverageRelativeHumidity,DAILYAverageDewPointTemp,DAILYAverageWetBulbTemp,DAILYHeatingDegreeDays,DAILYCoolingDegreeDays,DAILYSunrise,DAILYSunset,DAILYWeather,DAILYPrecip,DAILYSnowfall,DAILYSnowDepth,DAILYAverageStationPressure,DAILYAverageSeaLevelPressure,DAILYAverageWindSpeed,DAILYPeakWindSpeed,PeakWindDirection,DAILYSustainedWindSpeed,DAILYSustainedWindDirection,MonthlyMaximumTemp,MonthlyMinimumTemp,MonthlyMeanTemp,MonthlyAverageRH,MonthlyDewpointTemp,MonthlyWetBulbTemp,MonthlyAvgHeatingDegreeDays,MonthlyAvgCoolingDegreeDays,MonthlyStationPressure,MonthlySeaLevelPressure,MonthlyAverageWindSpeed,MonthlyTotalSnowfall,MonthlyDeptFromNormalMaximumTemp,MonthlyDeptFromNormalMinimumTemp,MonthlyDeptFromNormalAverageTemp,MonthlyDeptFromNormalPrecip,MonthlyTotalLiquidPrecip,MonthlyGreatestPrecip,MonthlyGreatestPrecipDate,MonthlyGreatestSnowfall,MonthlyGreatestSnowfallDate,MonthlyGreatestSnowDepth,MonthlyGreatestSnowDepthDate,MonthlyDaysWithGT90Temp,MonthlyDaysWithLT32Temp,MonthlyDaysWithGT32Temp,MonthlyDaysWithLT0Temp,MonthlyDaysWithGT001Precip,MonthlyDaysWithGT010Precip,MonthlyDaysWithGT1Snow,MonthlyMaxSeaLevelPressureValue,MonthlyMaxSeaLevelPressureDate,MonthlyMaxSeaLevelPressureTime,MonthlyMinSeaLevelPressureValue,MonthlyMinSeaLevelPressureDate,MonthlyMinSeaLevelPressureTime,MonthlyTotalHeatingDegreeDays,MonthlyTotalCoolingDegreeDays,MonthlyDeptFromNormalHeatingDD,MonthlyDeptFromNormalCoolingDD,MonthlyTotalSeasonToDateHeatingDD,MonthlyTotalSeasonToDateCoolingDD".split(",")
    // var cnt = -1
    // val admissionDataSchema = StructType(Array.fill(columnNames.size){
    //                                                       cnt+=1
    //                                                       if(cnt != 0 && cnt != 1 && cnt != 6)
    //                                                         StructField(columnNames(cnt), DoubleType)
    //                                                       else
    //                                                         StructField(columnNames(cnt), StringType)
    // })
    def parseInt(s: String) = try { Some(s.toInt) } catch { case _ => None }
    def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }
    val toInt = udf[Int, String]{n =>
      if(parseInt(n) == None) {
        0
      } else {
        n.toInt
      }
    }
    val toDouble = udf[Double, String]{ double =>
      if(parseDouble(double) == None) {
        println(double + "!!!!! double"*20)
        0.0
      } else {
        double.toDouble
      }
    }
    case class StormData(day: Int, month: String, year: Int, cat: String, cpoa: String, name: String);
    val sttStorms = spark.read.schema(Encoders.product[StormData].schema)
                        .option("delimiter", " ")
                        .csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/finalproject/data/sttStorms.txt")
    val stxStorms = spark.read.schema(Encoders.product[StormData].schema)
                        .option("delimiter", " ")
                        .csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/finalproject/data/stxStorms.txt")
    val prStorms = spark.read.schema(Encoders.product[StormData].schema)
                        .option("delimiter", " ")
                        .csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/finalproject/data/prStorms.txt")
    val allStorms = spark.read.schema(Encoders.product[StormData].schema)
                        .option("delimiter", " ")
                        .csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/finalproject/data/storms.txt")
    val weatherData = spark.read
                        .option("inferSchema", "true")
                        .option("header", "true")
                        .csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/finalproject/data/weather-data.csv")
                        .withColumn("year", when($"date".isNull && $"date" != $"DATE", null).otherwise(toInt($"date".substr(0, 4))))
                        .withColumn("month",when($"date".isNull && $"date" != $"DATE", null).otherwise(toInt($"date".substr(6, 2))))
                        .withColumn("day",when($"date".isNull && $"date" != $"DATE", null).otherwise(toInt($"date".substr(9, 2))))

    case class HourlyWeatherData(station_code: String, date: String, year: Int, month: Int, day: Int, weather_type: String, temp: Double, temp_dew_pnt: Double,
                                 humidity: Double, wind_speed: Double, pressure: Double,
                                 pressure_change: Double, pressure_sealvl: Double, precip: Double);
    case class DailyWeatherData(station_code: String, year: Int, month: Int, day: Int, temp: Double, temp_max: Double,
                                temp_min: Double, humidity: Double, temp_dew_pnt: Double, weather: String,
                                precip: Double, pressure: Double, pressure_sealvl: Double, wind_speed: Double,
                                peak_wind: Double, wind_speed_sustained: Double);

    val possibs = List("M", "*", "HOURLYWindSpeed", "HOURLYRelativeHumidity", "HOURLYPrecip", "DATE")
    // General Data analysis
    val hourlyData = weatherData
                    .withColumn("DATE", $"DATE".substr(0,10))
                    .withColumn("HOURLYDRYBULBTEMPF", when($"HOURLYDRYBULBTEMPF".contains("s") || $"HOURLYDRYBULBTEMPF".substr(6, 2).contains(".") || $"HOURLYDRYBULBTEMPF".contains("T") || $"HOURLYDRYBULBTEMPF".isin(possibs:_*) || $"HOURLYDRYBULBTEMPF".isNull , null).otherwise(toDouble($"HOURLYDRYBULBTEMPF")))
                    .withColumn("HOURLYDewPointTempF", when($"HOURLYDewPointTempF".contains("s") || $"HOURLYDewPointTempF".substr(6, 2).contains(".") || $"HOURLYDewPointTempF".contains("T") || $"HOURLYDewPointTempF".isin(possibs:_*) || $"HOURLYDewPointTempF".isNull, null).otherwise(toDouble($"HOURLYDewPointTempF")))
                    .withColumn("HOURLYRelativeHumidity", when($"HOURLYRelativeHumidity".contains("s") || $"HOURLYRelativeHumidity".substr(6, 2).contains(".") || $"HOURLYRelativeHumidity".contains("T") || $"HOURLYRelativeHumidity".isin(possibs:_*) || $"HOURLYRelativeHumidity".isNull, null).otherwise(toDouble($"HOURLYRelativeHumidity")))
                    .withColumn("HOURLYWindSpeed", when($"HOURLYWindSpeed".contains("s") || $"HOURLYWindSpeed".substr(6, 2).contains(".")|| $"HOURLYWindSpeed".contains("T") || $"HOURLYWindSpeed".isin(possibs:_*) || $"HOURLYWindSpeed".isNull, null).otherwise(toDouble($"HOURLYWindSpeed")))
                    .withColumn("HOURLYStationPressure", when($"HOURLYStationPressure".contains("s") || $"HOURLYStationPressure".substr(6, 2).contains(".") || $"HOURLYStationPressure".contains("T") || $"HOURLYStationPressure".isin(possibs:_*) || $"HOURLYStationPressure".isNull, null).otherwise(toDouble($"HOURLYStationPressure")))
                    .withColumn("HOURLYPressureChange", when($"HOURLYPressureChange".contains("s") || $"HOURLYPressureChange".substr(6, 2).contains(".") || $"HOURLYPressureChange".contains("T") || $"HOURLYPressureChange".isin(possibs:_*) || $"HOURLYPressureChange".isNull, null).otherwise(toDouble($"HOURLYPressureChange")))
                    .withColumn("HOURLYSeaLevelPressure", when($"HOURLYSeaLevelPressure".contains("s") || $"HOURLYSeaLevelPressure".substr(6, 2).contains(".") || $"HOURLYSeaLevelPressure".contains("T") || $"HOURLYSeaLevelPressure".isin(possibs:_*) || $"HOURLYSeaLevelPressure".isNull, null).otherwise(toDouble($"HOURLYSeaLevelPressure")))
                    .withColumn("HOURLYPrecip", when($"HOURLYPrecip".contains("s") || $"HOURLYPrecip".substr(6, 2).contains(".") || $"HOURLYPrecip".contains("T") || $"HOURLYPrecip".isin(possibs:_*) || $"HOURLYPrecip".isNull, null).otherwise(toDouble($"HOURLYPrecip")))
                    .select($"STATION".as("station_code"), $"DATE".as("date"), $"year", $"month", $"day",
                            $"HOURLYPRSENTWEATHERTYPE".as("weather_type"),
                            $"HOURLYDRYBULBTEMPF".as("temp"),
                            $"HOURLYDewPointTempF".as("temp_dew_pnt"),
                            $"HOURLYRelativeHumidity".as("humidity"),
                            $"HOURLYWindSpeed".as("wind_speed"),
                            $"HOURLYStationPressure".as("pressure"),
                            $"HOURLYPressureChange".as("pressure_change"),
                            $"HOURLYSeaLevelPressure".as("pressure_sealvl"),
                            $"HOURLYPrecip".as("precip"))
                    .as[HourlyWeatherData]

    // hourlyData.summary().show()

   //  println(hourlyData.count())
   //  val infoByStation = hourlyData.groupBy($"station_code")
   //                              .agg(count($"station_code"))
   //  val stxData = hourlyData.filter($"station_code" === "WBAN:11624")
   //  stxData.groupBy($"year").agg(count($"year")).show(true)
   //  val sttData = hourlyData.filter($"station_code" === "WBAN:11640")
   //  sttData.groupBy($"year").agg(count($"year")).show(true)
   //  val prData1 = hourlyData.filter($"station_code" === "WBAN:11630")
   //  prData1.groupBy($"year").agg(count($"year")).show(true)
   //  val prData2 = hourlyData.filter($"station_code" === "WBAN:11641")
   //  prData2.groupBy($"year").agg(count($"year")).show(true)
   //
   //  infoByStation.show()
   //
   //  //Scrap hurricane data
   //  // Strongest hurricanes/weakest storms --> Will categorize storms like this for future
   //  val monthsMap = Map("Jan"->1,"Feb"->2,"Mar"->3,"Apr"->4,"May"->5,"Jun"->6,"Jul"->7,"Aug"->8,"Sep"->9,"Oct"->10,"Nov"->11,"Dec"->12)
   //  val monthAsNumber = udf[Int, String]{n => monthsMap(n)}
   //  val sttStormDays = sttStorms.withColumn("month", monthAsNumber($"month"))
   //                             .select(concat($"year", lit("-"), $"month", lit("-"), $"day"))
   //                             .map(_.getString(0)).collect()
   // val stxStormDays = stxStorms.withColumn("month", monthAsNumber($"month"))
   //                            .select(concat($"year", lit("-"), $"month", lit("-"), $"day"))
   //                            .map(_.getString(0)).collect()
   //  val prStormDays = prStorms.withColumn("month", monthAsNumber($"month"))
   //                             .select(concat($"year", lit("-"), $"month", lit("-"), $"day"))
   //                             .map(_.getString(0)).collect()
   //  val sttDataStorms = sttData.filter($"date".isin(sttStormDays:_*))
   //                          .groupBy($"date").agg(avg("precip"), avg("pressure"), avg("wind_speed"))
   //  val stxDataStorms = stxData.filter($"date".isin(stxStormDays:_*))
   //                          .groupBy($"date").agg(avg("precip"), avg("pressure"), avg("wind_speed"))
   //  val prData1Storms = prData1.filter($"date".isin(prStormDays:_*))
   //                          .groupBy($"date").agg(avg("precip"), avg("pressure"), avg("wind_speed"))
   //  val prData2Storms = prData2.filter($"date".isin(prStormDays:_*))
   //                          .groupBy($"date").agg(avg("precip"), avg("pressure"), avg("wind_speed"))
   //
   //  // val hugo = stxData.filter($"year"=== 1989 && $"day" === 18 && $"month" = 9).show()
   //  sttDataStorms.show(10)
   //  stxDataStorms.show(10)
   //  prData1Storms.show(10)
   //  prData2Storms.show(10)
   //
   //  println(sttData.count)

    // Graph the precip, pressure, and winds of all the hurricanes
    val windiestDays = hourlyData.groupBy($"date").agg(avg($"wind_speed").as("wind_speed"),
                                                      avg($"temp").as("temp"),
                                                      avg($"pressure").as("pressure"),
                                                      avg($"pressure_sealvl").as("pressure_sealvl"),
                                                      avg($"humidity").as("humidity"))
                                                  .orderBy($"wind_speed".desc)
    windiestDays.show(20)

    val hPlotData = hourlyData.groupBy($"date").agg(avg($"wind_speed").as("wind_speed"))
                                               .withColumn("doy", toInt($"date".substr(6, 2)) * 31 + toInt($"date".substr(9, 2)))
                                               .filter($"wind_speed".isNotNull && $"doy".isNotNull)
    val winds = hPlotData.select($"wind_speed").map(_.getDouble(0)).collect()
    val doys = hPlotData.select($"doy").map(_.getInt(0)).collect()
    var cg = ColorGradient(0.0 -> BlueARGB)
    val hurricanesPlot = Plot.scatterPlotGrid(Seq(Seq((doys, winds, winds.map(cg), 10))), "", "Days of Year", "Wind Speed (mph)")
    SwingRenderer(hurricanesPlot, 800, 800, true)

  //  Clustering --> Hurricane season vs Regular Season
    val dailyData = hourlyData.groupBy($"month").agg(avg($"wind_speed").as("wind_speed"),
                                                avg($"temp").as("temp"),
                                                avg($"pressure").as("pressure"),
                                                avg($"pressure_sealvl").as("pressure_sealvl"),
                                                avg($"humidity").as("humidity"),
                                                avg($"pressure_change").as("pressure_change"),
                                                avg($"precip").as("precip"))
    dailyData.show(5)
    println(dailyData.printSchema())
    val cols = dailyData.columns.filter(c => c != "month" && c!="year" && c!="day")
    val imputer = new Imputer()
        .setInputCols(cols)
        .setOutputCols(cols.map(c => s"${c}_imputed"))
        .setStrategy("mean")
    val imputedData = imputer.fit(dailyData).transform(dailyData)
    val clusterDataVA = new VectorAssembler().setInputCols(cols.map(c => s"${c}_imputed")).setOutputCol("params")
    val clusterDataWithParams = clusterDataVA.transform(imputedData)
    val kMeans2K = new KMeans().setK(2).setFeaturesCol("params")
    val clusterDataModel = kMeans2K.fit(clusterDataWithParams)
    def getCluster2K(month: Int): Int = {
      if(month >= 6 && month <= 11) 0 else 1
    }
    val getCluster2KUDF = udf[Int,Int] (getCluster2K)
    val clusterDataWithClusters = clusterDataModel.transform(clusterDataWithParams)
                                                  .withColumn("cluster", getCluster2KUDF($"month"))
    clusterDataWithClusters.show(5)
    val fractionCorrect = clusterDataWithClusters.filter($"prediction" === $"cluster").count().toDouble / clusterDataWithClusters.count()
    println("FractionCorrect: " + fractionCorrect)

    val preds = clusterDataWithClusters.select($"prediction").map(_.getInt(0)).collect().map(_.toDouble)
    val clusters = clusterDataWithClusters.select($"cluster").map(_.getInt(0)).collect().map(_.toDouble)
    val pressures = clusterDataWithClusters.select($"pressure_sealvl_imputed").map(_.getDouble(0)).collect()
    val humids = clusterDataWithClusters.select($"wind_speed_imputed").map(_.getDouble(0)).collect()
    val ccg = ColorGradient(0.0 -> RedARGB, 1.0 -> GreenARGB)
    val clusterPlot = Plot.scatterPlotGrid(  Seq(Seq((pressures, humids, clusters.map(ccg), 15), (pressures, humids, preds.map(ccg), 15))),
                                            "", "Sea Level Pressure (Hg)", "Wind Speed (mph)")
    SwingRenderer(clusterPlot, 800, 800, true)
    // What values changed the most when the hurricanes were approaching -- monthly

    val classData = hourlyData.groupBy($"year", $"month", $"day").agg(avg($"wind_speed").as("wind_speed"),
                                                avg($"temp").as("temp"),
                                                avg($"pressure").as("pressure"),
                                                avg($"pressure_sealvl").as("pressure_sealvl"),
                                                avg($"humidity").as("humidity"),
                                                avg($"pressure_change").as("pressure_change"),
                                                avg($"precip").as("precip"))
    classData.summary().show()
    val classCols = classData.columns.filter(c => c != "month" && c!="year" && c!="day" && c!="wind_speed")
    val imper = new Imputer()
        .setInputCols(cols)
        .setOutputCols(cols.map(c => s"${c}_imputed"))
        .setStrategy("mean")
    val impClass = imper.fit(classData).transform(classData)
    val classDataVA = new VectorAssembler().setInputCols(classCols.map(c => s"${c}_imputed")).setOutputCol("params")
    val classDataWithParams = classDataVA.transform(impClass)
    val Array(clTrain, clTest) = classDataWithParams.randomSplit(Array(0.8, 0.2))
    val clRegression = new DecisionTreeRegressor().setFeaturesCol("params").setLabelCol("wind_speed_imputed")
    val clModel = clRegression.fit(clTrain)
    val clPred = clModel.transform(clTest)
    clPred.select($"prediction", $"wind_speed_imputed", $"params").show()
    val clEval = new RegressionEvaluator().setLabelCol("wind_speed_imputed").setPredictionCol("prediction").
                    setMetricName("rmse")
    println(clEval.evaluate(clPred))
    val clEval1 = new RegressionEvaluator().setLabelCol("wind_speed_imputed").setPredictionCol("prediction").
                    setMetricName("r2")
    println(clEval1.evaluate(clPred))

    val importances = clModel.featureImportances.toArray
                             .zip(classCols)
                             .sortBy(_._1)(Ordering[Double].reverse)
    importances.foreach(println)

    // How have those values changed throughout the dataset
    val pData = hourlyData.groupBy($"year").agg(avg($"wind_speed").as("wind_speed"),
                                                avg($"temp").as("temp"),
                                                avg($"pressure").as("pressure"),
                                                avg($"pressure_sealvl").as("pressure_sealvl"),
                                                avg($"humidity").as("humidity"),
                                                avg($"pressure_change").as("pressure_change"),
                                                avg($"precip").as("precip"))
                                                .filter($"wind_speed".isNotNull && $"pressure_sealvl".isNotNull && $"Humidity".isNotNull)
    val yrs = pData.select($"year").map(_.getInt(0)).collect()
    val psls = pData.select($"pressure_sealvl").map(_.getDouble(0)).collect()
    val pcg = ColorGradient(0.0 -> RedARGB)
    val pressurePlot = Plot.scatterPlotGrid(  Seq(Seq((yrs, pressures, psls.map(pcg), 15))),
                                            "", "Years", "Sea Level Pressure (Hg)")
    SwingRenderer(pressurePlot, 800, 800, true)

    val ws = pData.select($"wind_speed").map(_.getDouble(0)).collect()
    val windsPlot = Plot.scatterPlotGrid(  Seq(Seq((yrs, ws, psls.map(pcg), 15))),
                                            "", "Years", "Wind Speed (mph)")
    SwingRenderer(windsPlot, 800, 800, true)

    val hs = pData.select($"humidity").map(_.getDouble(0)).collect()
    val hsPlot = Plot.scatterPlotGrid(  Seq(Seq((yrs, hs, psls.map(pcg), 15))),
                                            "", "Years", "Humidity %")
    SwingRenderer(hsPlot, 800, 800, true)

    // val lstData = hourlyData.groupBy($"year", $"month", $"date").agg(avg($"wind_speed").as("wind_speed"),
    //                                             avg($"temp").as("temp"),
    //                                             avg($"pressure").as("pressure"),
    //                                             avg($"pressure_sealvl").as("pressure_sealvl"),
    //                                             avg($"humidity").as("humidity"),
    //                                             avg($"pressure_change").as("pressure_change"),
    //                                             avg($"precip").as("precip"))
    //                                             .filter($"wind_speed".isNotNull && $"pressure_sealvl".isNotNull && $"Humidity".isNotNull)
    //                                             .withColumn("year")

    println("\n!!!!!!!!" * 6)
//  }
}
