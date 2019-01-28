package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql._
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._

object LocalAreaUnemployment extends App {
  val spark = SparkSession.builder().master("local[*]").appName("LocalAreaUnemployment").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  val laDataSchema = StructType(Array(
      StructField("series_id", StringType),
      StructField("year", IntegerType),
      StructField("period", StringType),
      StructField("value", DoubleType),
      StructField("footnote_codes", StringType)
      ))

  val newMexicoSeries = spark.read.schema(laDataSchema).
            option("header","true").
            option("delimiter", "\t").
            csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.data.38.NewMexico")

  val series = spark.read.
            option("inferSchema", "true").
            option("header","true").
            option("delimiter", "\t").
            csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.series")

  val cities = spark.read.
            option("inferSchema", "true").
            option("header","true").
            option("delimiter", "\t").
            csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.data.65.City")

  val counties = spark.read.schema(laDataSchema).
            option("header","true").
            option("delimiter", "\t").
            csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.data.64.County")

  val areas = spark.read.
            option("inferSchema", "true").
            option("header","true").
            option("delimiter", "\t").
            csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.area")

  val texasSeries = spark.read.
            option("inferSchema", "true").
            option("header","true").
            option("delimiter", "\t").
            csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.data.51.Texas")

  val allStates = spark.read.schema(laDataSchema).
            option("header","true").
            option("delimiter", "\t").
            csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.data.concatenatedStateFiles")
  val zipCodesStates = spark.read.
            option("inferSchema", "true").
            option("header","true").
            csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/zip_codes_states.csv")


  //1. How many data series does the state of New Mexico have?
  println(newMexicoSeries.count())
  //answer: 99496
  //2. What is the highest unemployment level (not rate) reported for a county or equivalent in the time series for the state of New Mexico?
  newMexicoSeries.filter($"series_id".substr(19,2) === "04").
                  select($"series_id".substr(4,15).as("area_code"), $"value").
                  createOrReplaceTempView("unemploymentNewMexico")
  areas.filter($"area_type_code" === "F").createOrReplaceTempView("counties")
  spark.sql("""
    SELECT *
    FROM unemploymentNewMexico, counties
    WHERE counties.area_code = unemploymentNewMexico.area_code
    ORDER BY unemploymentNewMexico.value
  """).show()
  //answer: 28461
  //3. How many cities/towns with more than 25,000 people does the BLS track in New Mexico?
  areas.filter($"area_type_code" === "G").createOrReplaceTempView("citiesOver25")
  newMexicoSeries.select($"series_id".substr(4,15).as("area_code")).distinct().createOrReplaceTempView("newMexicoAreaCodes")
  spark.sql("""
    SELECT count(*)
    FROM citiesOver25, newMexicoAreaCodes
    WHERE citiesOver25.area_code == newMexicoAreaCodes.area_code
  """).show()
  //answer: 10
  //4. What was the average unemployment rate for New Mexico in 2017? Calculate this in three ways:
  //a. Averages of the months for the BLS series for the whole state.

  //USE M13
  newMexicoSeries.filter($"series_id".substr(19,2) === "03" && $"year" === 2017 && !($"period" === "M13")).
                  select($"series_id".substr(4,15).as("area_code"), $"value", $"period", $"year", $"series_id".substr(1, 18).as("series_id")).
                  createOrReplaceTempView("unemploymentRateNewMexico")
  newMexicoSeries.filter($"series_id".substr(19,2) === "03" && $"year" === 2017 && ($"period" === "M13")).
                  select($"series_id".substr(4,15).as("area_code"), $"value", $"period", $"year", $"series_id".substr(1, 18).as("series_id")).
                  createOrReplaceTempView("unemploymentRateNewMexicoM13")
  spark.sql("""
    SELECT avg(value), period
    FROM unemploymentRateNewMexico
    GROUP BY period
    ORDER BY period
  """).show()

  spark.sql("""
    SELECT avg(value)
    FROM unemploymentRateNewMexicoM13
  """).show()

  //b. Simple average of all unemployment rates for counties in the state.
  spark.sql("""
    SELECT avg(unemploymentRateNewMexicoM13.value)
    FROM unemploymentRateNewMexicoM13, counties
    WHERE unemploymentRateNewMexicoM13.area_code = counties.area_code
  """).show()
  spark.sql("""
    SELECT avg(unemploymentRateNewMexico.value)
    FROM unemploymentRateNewMexico, counties
    WHERE unemploymentRateNewMexico.area_code = counties.area_code
  """).show()

//5. This is a continuation of the last question. Calculate the unemployment rate in a third way and discuss the differences.
//c. Weighted average of all unemployment rates for counties in the state where the weight is the labor force in that month.
newMexicoSeries.filter($"series_id".substr(19,2) === "06" && !($"period" == "M13")).
                select($"value", $"period", $"year", $"series_id".substr(1,18).as("series_id")).
                createOrReplaceTempView("laborForceNMByMonth")

spark.sql("""
  SELECT unemploymentRateNewMexico.value, unemploymentRateNewMexico.period, unemploymentRateNewMexico.year, unemploymentRateNewMexico.series_id
  FROM unemploymentRateNewMexico, counties
  WHERE unemploymentRateNewMexico.area_code = counties.area_code
""").createOrReplaceTempView("unemploymentRateNMByCounty")

val weightedValues = spark.sql("""
  SELECT (laborForceNMByMonth.value * unemploymentRateNMByCounty.value) as values, laborForceNMByMonth.value as weight
  FROM laborForceNMByMonth, unemploymentRateNMByCounty
  WHERE laborForceNMByMonth.period = unemploymentRateNMByCounty.period AND
        laborForceNMByMonth.year = unemploymentRateNMByCounty.year AND
        laborForceNMByMonth.series_id = unemploymentRateNMByCounty.series_id
""")
println(weightedValues.count())
println(weightedValues.agg(sum($"values")).first.getDouble(0)/weightedValues.agg(sum($"weight")).first.getDouble(0))
//d. How do your two averages compare to the BLS average? Which is more accurate and why?

//6. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the state of Texas? When and where? (raise labor force limit for next year)
//period, year, value of highest unemployment rate with labor force of 10,000 or more in Texas.
texasSeries.filter($"series_id".substr(19,2) === "06" && $"value" >= 10000).
            select($"series_id".substr(1,18).as("series_id")).distinct().
            createOrReplaceTempView("lFOverMinTexas")
texasSeries.filter($"series_id".substr(19,2) === "03").
            select($"series_id".substr(1,18).as("series_id"), $"period", $"year", $"value").
            createOrReplaceTempView("uRTexas")
spark.sql("""
  SELECT uRTexas.value, uRTexas.period, uRTexas.year, uRTexas.series_id
  FROM uRTexas, lFOverMinTexas
  WHERE uRTexas.series_id = lFOverMinTexas.series_id
  ORDER BY uRTexas.value DESC
""").show()

//The following questions involve all the states.
//7. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the full dataset? When and where? (raise labor force limit for next year)
//allStates.describe().show()
allStates.filter($"series_id".substr(19,2) === "06" && $"value" >= 10000).
          select($"series_id".substr(1,18).as("series_id")).distinct().
          createOrReplaceTempView("lFOverMinAllStates")
allStates.filter($"series_id".substr(19,2) === "03").
          select($"series_id".substr(1,18).as("series_id"), $"period", $"year", $"value").
          createOrReplaceTempView("uRAllStates")
spark.sql("""
  SELECT uRAllStates.value, uRAllStates.period, uRAllStates.year, uRAllStates.series_id
  FROM uRAllStates, lFOverMinAllStates
  WHERE uRAllStates.series_id = lFOverMinAllStates.series_id
  ORDER BY uRAllStates.value DESC
""").show()

//8. Which state has most distinct data series? How many series does it have?
series.select($"srd_code", $"series_id").
       groupBy($"srd_code").
       agg(count($"series_id")).show()

//9. We will finish up by looking at unemployment geographically and over time. I want you to make a grid of scatter plots for the years
//2000, 2005, 2010, and 2015. Each point is plotted at the X, Y for latitude and longitude and it should be colored by the unemployment
//rate. If you are using SwiftVis2, the Plot.scatterPlotGrid method is particularly suited for this. Only plot data from the continental
//US, so don't include data from Alaska, Hawaii, or Puerto Rico.

//Leave out Hawaii, Alaska, Puerto Rico, etc.

var countyData = counties.filter($"series_id".substr(19,2) === "03" && $"period" === "M13" && $"year" === 2000).
                 select($"series_id".substr(4,15).as("area_code"), $"value", $"year").
                 createOrReplaceTempView("countyData")
var countyNamesAndCodes = areas.filter($"area_type_code" === "F").
                          select($"area_text", $"area_code").distinct().
                          createOrReplaceTempView("countyNamesAndCodes")
var namesAndValues = spark.sql("""
  SELECT countyNamesAndCodes.area_text, countyData.value, countyData.year
  FROM countyNamesAndCodes, countyData
  WHERE countyNamesAndCodes.area_code = countyData.area_code
""")
val filteredStatesData = zipCodesStates.filter($"state" =!= "PR" && $"state" =!= "VI" && $"state" =!= "HI" && $"state" =!= "AK")
val mapData2000 = namesAndValues.join(filteredStatesData, $"area_text".contains($"county")).filter($"longitude".isNotNull && $"latitude".isNotNull && $"value".isNotNull)

countyData = counties.filter($"series_id".substr(19,2) === "03" && $"period" === "M13" && $"year" === 2005).
                 select($"series_id".substr(4,15).as("area_code"), $"value", $"year").
                 createOrReplaceTempView("countyData")
countyNamesAndCodes = areas.filter($"area_type_code" === "F").
                          select($"area_text", $"area_code").distinct().
                          createOrReplaceTempView("countyNamesAndCodes")
namesAndValues = spark.sql("""
  SELECT countyNamesAndCodes.area_text, countyData.value, countyData.year
  FROM countyNamesAndCodes, countyData
  WHERE countyNamesAndCodes.area_code = countyData.area_code
""")
val mapData2005 = namesAndValues.join(filteredStatesData, $"area_text".contains($"county")).filter($"longitude".isNotNull && $"latitude".isNotNull && $"value".isNotNull)

countyData = counties.filter($"series_id".substr(19,2) === "03" && $"period" === "M13" && $"year" === 2010).
                 select($"series_id".substr(4,15).as("area_code"), $"value", $"year").
                 createOrReplaceTempView("countyData")
countyNamesAndCodes = areas.filter($"area_type_code" === "F").
                          select($"area_text", $"area_code").distinct().
                          createOrReplaceTempView("countyNamesAndCodes")
namesAndValues = spark.sql("""
  SELECT countyNamesAndCodes.area_text, countyData.value, countyData.year
  FROM countyNamesAndCodes, countyData
  WHERE countyNamesAndCodes.area_code = countyData.area_code
""")
val mapData2010 = namesAndValues.join(filteredStatesData, $"area_text".contains($"county")).filter($"longitude".isNotNull && $"latitude".isNotNull && $"value".isNotNull)

countyData = counties.filter($"series_id".substr(19,2) === "03" && $"period" === "M13" && $"year" === 2015).
                 select($"series_id".substr(4,15).as("area_code"), $"value", $"year").
                 createOrReplaceTempView("countyData")
countyNamesAndCodes = areas.filter($"area_type_code" === "F").
                          select($"area_text", $"area_code").distinct().
                          createOrReplaceTempView("countyNamesAndCodes")
namesAndValues = spark.sql("""
  SELECT countyNamesAndCodes.area_text, countyData.value, countyData.year
  FROM countyNamesAndCodes, countyData
  WHERE countyNamesAndCodes.area_code = countyData.area_code
""")
val mapData2015 = namesAndValues.join(filteredStatesData, $"area_text".contains($"county")).filter($"longitude".isNotNull && $"latitude".isNotNull && $"value".isNotNull)

val cg = ColorGradient(0.0 -> GreenARGB, 6.0 -> YellowARGB, 12.0 -> RedARGB)
val longs2000 = mapData2000.select($"longitude").map(_.getDouble(0)).collect()
val lats2000 = mapData2000.select($"latitude").map(_.getDouble(0)).collect()
val vals2000 = mapData2000.select($"value").map(_.getDouble(0)).collect()

val longs2005 = mapData2005.select($"longitude").map(_.getDouble(0)).collect()
val lats2005 = mapData2005.select($"latitude").map(_.getDouble(0)).collect()
val vals2005 = mapData2005.select($"value").map(_.getDouble(0)).collect()

val longs2010 = mapData2010.select($"longitude").map(_.getDouble(0)).collect()
val lats2010 = mapData2010.select($"latitude").map(_.getDouble(0)).collect()
val vals2010 = mapData2010.select($"value").map(_.getDouble(0)).collect()

val longs2015 = mapData2015.select($"longitude").map(_.getDouble(0)).collect()
val lats2015 = mapData2015.select($"latitude").map(_.getDouble(0)).collect()
val vals2015 = mapData2015.select($"value").map(_.getDouble(0)).collect()
val plot = Plot.scatterPlotGrid(
  Seq(
    Seq((longs2000, lats2000, vals2000.map(cg), 5), (longs2005, lats2005, vals2005.map(cg), 5)),
    Seq((longs2010, lats2010, vals2010.map(cg), 5), (longs2015, lats2015, vals2015.map(cg), 5))),
  "Unemployment Rates By Year", "Longitude", "Latitude")
  SwingRenderer(plot, 800, 800, true)

spark.sparkContext.stop()
}
