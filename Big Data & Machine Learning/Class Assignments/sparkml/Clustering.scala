package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql._
import org.apache.spark.ml.linalg
import scala.collection.mutable.Map
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.Imputer
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._

case class VotingData(votes_id: Int, votes_dem: Double, votes_gop: Double,
                      total_votes: Double, per_dem: Double, per_gop: Double,
                      diff: Int, per_point_diff: Double, state_abbr: String,
                      county_name: String, combined_fips: Int);
case class VotingDataString(votes_id: Int, votes_dem: Double, votes_gop: Double,
                      total_votes: Double, per_dem: Double, per_gop: Double,
                      diff: String, per_point_diff: String, state_abbr: String,
                      county_name: String, combined_fips: Int);
case class UnemploymentData(series_id: String, year: Int, period: String, value: Double, footnote: String);
case class AreaData(area_type_code: String, area_code: String, area_text: String,
                    display_level: Int, selectable: String, sort_sequence: String);
case class ZipCodeData(zip_code: Int, latitude: Double, longitude: Double,
                       city: String, state: String, county: String);

object Clustering extends App{
  //def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkClustering").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("!!!!!!!!\n" * 10)

    val wageColumnInfo = spark.read.
                  option("header", "true").
                  option("inferSchema", "true").
                  csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparkml/data/clustering/qcew/csv_quarterly_layout.csv.csv")
    val fieldNames = wageColumnInfo.select($"field_name").map(_.getString(0)).collect()
    val dataTypes = wageColumnInfo.select($"data_type").map(_.getString(0)).collect()
    val dataTypesMap = Map("Text" -> StringType, "Numeric" -> DoubleType)
    var cnt = -1
    val wageDataSchema = StructType(Array.fill(fieldNames.size){cnt+=1
                                                          StructField(fieldNames(cnt), dataTypesMap(dataTypes(cnt)), true)
    })
    val wageData = spark.read.schema(wageDataSchema).
                  option("header", "true").
                  csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparkml/data/clustering/qcew/2016.q1-q4.singlefile.csv")

    def commaRemover(s: String): Int = {s.replaceAll(",", "").toInt}
    def percentRemover(s: String): Double = {s.replace("%", "").toDouble}
    val commaRemoverUDF = udf[Int, String] (commaRemover)
    val percentRemoverUDF = udf[Double, String] (percentRemover)

    val vd = spark.read.schema(Encoders.product[VotingDataString].schema).
              option("header", "true").
              csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql2/data/2016_US_County_Level_Presidential_Results.csv").
              as[VotingDataString]
    val voteData = vd.withColumn("diff", commaRemoverUDF($"diff")).
                      withColumn("per_point_diff", percentRemoverUDF($"per_point_diff")).
                      as[VotingData]
    val allStates = spark.read.schema(Encoders.product[UnemploymentData].schema).
              option("header","true").
              option("delimiter", "\t").
              csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.data.concatenatedStateFiles").
              as[UnemploymentData]
    val areas = spark.read.
                option("header","true").schema(Encoders.product[AreaData].schema).
                option("delimiter", "\t").
                csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.area").
                as[AreaData]
    val zipCodesStates = spark.read.schema(Encoders.product[ZipCodeData].schema).
                 option("header","true").
                 csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/zip_codes_states.csv").
                 as[ZipCodeData]

    // //#1
    // val countyLvlCnts = wageData.filter($"agglvl_code" >= 70 && $"agglvl_code" <= 78).
    //                              groupBy($"agglvl_code").agg(count($"agglvl_code")).show()
    // //#2
    // val bexarCountyCnt = wageData.filter($"area_fips" === 48029).count()
    // println(bexarCountyCnt)
    //
    // //#3
    // val industryCodeCnts = wageData.groupBy($"industry_code").agg(count($"industry_code").as("cnts")).orderBy($"cnts".desc).show()
    //
    // //#4
    // val totalWages = wageData.filter($"agglvl_code" === 78 && $"year" === 2016).
    //                           groupBy($"industry_code").agg(sum($"total_qtrly_wages").as("sums"), count($"total_qtrly_wages")).
    //                           orderBy($"sums".desc).show()

    //#5
    //Get all county level unemployment data
    case class SeriesByCountyName(area_code: String, value: Double, area_text: String);
    case class SeriesLFUR(area_code: String, area_text: String, lf_value: Double, ur_value: Double);
    val countyCodes = areas.filter($"area_type_code" === "F")
    val allCountyUR = allStates.filter($"series_id".substr(19,2) === "03" && $"year" === 2016 && $"period" === "M13").
                      joinWith(countyCodes, $"series_id".substr(4,15) === $"area_code").
                      map(j => SeriesByCountyName(j._1.series_id.substring(3,18), j._1.value, j._2.area_text))
    val allCountyLF = allStates.filter($"series_id".substr(19,2) === "06" && $"year" === 2016 && $"period" === "M13").
                      joinWith(countyCodes, $"series_id".substr(4,15) === $"area_code").
                      map(j => SeriesByCountyName(j._1.series_id.substring(3,18), j._1.value, j._2.area_text))
    val allCountyUData = allCountyUR.joinWith(allCountyLF, allCountyUR("area_code") === allCountyLF("area_code")).
                                     map(j => SeriesLFUR(j._1.area_code.substring(2,7), j._1.area_text, j._2.value, j._1.value))

    case class CountyWageData(area_fips: String, c1: Double, c2: Double, c3: Double, c4: Double, c5: Double);
    val countyWageData = wageData.filter($"agglvl_code" === "70" && $"year" === "2016").groupBy($"area_fips").
                                  agg(avg($"lq_qtrly_contributions").as("c1"),
                                  avg($"lq_total_qtrly_wages").as("c2"),
                                  avg($"lq_taxable_qtrly_wages").as("c3"),
                                  avg($"lq_avg_wkly_wage").as("c4"),
                                  avg($"oty_total_qtrly_wages_pct_chg").as("c5")).as[CountyWageData]

    case class ClusterData(area_fips: Int, area_text: String, labor_force: Double, u_rate: Double,
                           c1: Double, c2: Double, c3: Double, c4: Double, c5: Double);
    val clusterData = allCountyUData.joinWith(countyWageData, $"area_code" === $"area_fips").
                                     map(j => ClusterData(j._2.area_fips.toInt, j._1.area_text, j._1.lf_value,
                                                          j._1.ur_value, j._2.c1, j._2.c2, j._2.c3, j._2.c4, j._2.c5))

    val clusterDataVA = new VectorAssembler().setInputCols(Array("labor_force", "u_rate", "c1", "c2", "c3", "c4", "c5")).setOutputCol("params")
    val clusterDataWithParams = clusterDataVA.transform(clusterData)
    val kMeans2K = new KMeans().setK(2).setFeaturesCol("params")
    val clusterDataModel = kMeans2K.fit(clusterDataWithParams)
    val clusterDataWithClusters = clusterDataModel.transform(clusterDataWithParams)

    val kMeans4K = new KMeans().setK(4).setFeaturesCol("params")
    val clusterDataModel4K = kMeans4K.fit(clusterDataWithParams)
    val clusterDataWithClusters4K = clusterDataModel4K.transform(clusterDataWithParams)

    case class ClusterCheck(area_fips: Int, prediction: Int);
    val clusterCheck = clusterDataWithClusters.select($"area_fips", $"prediction").as[ClusterCheck]
    val clusterCheck4K = clusterDataWithClusters4K.select($"area_fips", $"prediction").as[ClusterCheck]

    case class VotingDataWithCluster(votes_id: Int, votes_dem: Double, votes_gop: Double,
                          total_votes: Double, per_dem: Double, per_gop: Double,
                          diff: Int, per_point_diff: Double, state_abbr: String,
                          county_name: String, combined_fips: Int, cluster: Int);

    def getCluster2K(per_dem: Double, per_gop: Double): Int = {
      if(per_dem > per_gop) 1 else 0
    }
    val getCluster2KUDF = udf[Int,Double, Double] (getCluster2K)
    def getCluster4K(per_dem: Double, per_gop: Double): Int = {
      val frac = (per_dem*100) / (per_gop*100)
      if(frac <= 0.5) 0
      else if(frac < 1) 1
      else if(frac > 1 && frac <= 2) 2
      else 3
    }
    val getCluster4KUDF = udf[Int,Double, Double] (getCluster4K)
    val voteDataWithCluster2K= voteData.withColumn("cluster", getCluster2KUDF($"per_dem", $"per_gop")).
                               as[VotingDataWithCluster]
    voteDataWithCluster2K.select($"per_dem", $"per_gop", $"cluster").summary().show()
    val voteDataWithCluster4K= voteData.withColumn("cluster", getCluster4KUDF($"per_dem", $"per_gop")).
                               as[VotingDataWithCluster]

    case class JoinedData(cluster: Int, prediction: Int, county_name: String, state_abbr: String);
    val joinedData2K = voteDataWithCluster2K.joinWith(clusterCheck, $"area_fips" === $"combined_fips").
                                             map(j => (JoinedData(j._1.cluster, j._2.prediction, j._1.county_name, j._1.state_abbr)))
    val joinedData4K = voteDataWithCluster4K.joinWith(clusterCheck4K, $"area_fips" === $"combined_fips").
                                             map(j => (JoinedData(j._1.cluster, j._2.prediction, j._1.county_name, j._1.state_abbr)))
    clusterDataWithClusters.summary().show()
    joinedData2K.summary().show()
    clusterDataWithClusters4K.summary().show()
    joinedData4K.summary().show()
    val fractionCorrect = joinedData2K.filter($"cluster" === $"prediction").count().toDouble/joinedData2K.count()
    println("Fraction Correct2K: " + fractionCorrect)
    val fractionCorrect4K = joinedData4K.filter($"cluster" === $"prediction").count().toDouble/joinedData4K.count()
    println("Fraction Correct4K: " + fractionCorrect4K)

    //6.
    // Make scatter plots of the voting results of the 2016 election (I suggest ratio of votes for each party)
    val demsRatio = voteData.map(vd => vd.votes_dem / vd.total_votes).collect()
    val gopsRatio = voteData.map(vd => vd.votes_gop / vd.total_votes).collect()
    val electionRatioPlot = Plot.scatterPlots(Seq((demsRatio, gopsRatio, BlackARGB, 6)), "Party Voting Ratios for 2016 Election", "Democratic Ratio", "Republican Ratio")
    SwingRenderer(electionRatioPlot, 800, 800, true)

    //Correct cluster grid compared to predictions.
    case class AltZipData(latitude: Double, longitude: Double, state: String, county: String)
    case class MapData(latitude: Double, longitude: Double, cluster: Int, prediction: Int);
    val reformedZipCodeData = zipCodesStates.filter($"state" =!= "PR" && $"state" =!= "VI" && $"state" =!= "HI" && $"state" =!= "AK")
                                            .groupBy($"state", $"county")
                                            .agg(avg($"latitude").as("latitude"), avg($"longitude").as("longitude"))
                                            .as[AltZipData]
    val allMapData2K = joinedData2K.joinWith(reformedZipCodeData, $"county_name".contains($"county") && $"state_abbr" === $"state")
                                                     .map(jd => MapData(jd._2.latitude, jd._2.longitude, jd._1.cluster, jd._1.prediction))
    val allMapData4K = joinedData4K.joinWith(reformedZipCodeData, $"county_name".contains($"county") && $"state_abbr" === $"state")
                                                     .map(jd => MapData(jd._2.latitude, jd._2.longitude, jd._1.cluster, jd._1.prediction))

    var longs = allMapData2K.map(_.longitude).collect()
    var lats = allMapData2K.map(_.latitude).collect()
    var clusters = allMapData2K.map(_.cluster.toDouble).collect()
    var preds = allMapData2K.map(_.prediction.toDouble).collect()
    var cg = ColorGradient(0.0 -> RedARGB, 1.0 -> BlueARGB)
    val plot2K = Plot.scatterPlotGrid(Seq(Seq((longs, lats, clusters.map(cg), 10), (longs, lats, preds.map(cg), 10))),
        "Clusters vs Predictions", "Longitude", "Latitude")
    SwingRenderer(plot2K, 800, 800, true)

    longs = allMapData4K.map(_.longitude).collect()
    lats = allMapData4K.map(_.latitude).collect()
    clusters = allMapData4K.map(_.cluster.toDouble).collect()
    preds = allMapData4K.map(_.prediction.toDouble).collect()
    cg = ColorGradient(0.0 -> 0xFF910000, 1.0 -> 0xFFFF0000, 2.0 -> 0xFF000091, 3.0 -> 0xFF0000FF)
    val plot4K = Plot.scatterPlotGrid(Seq(Seq((longs, lats, clusters.map(cg), 10), (longs, lats, preds.map(cg), 10))),
        "Clusters vs Predictions", "Longitude", "Latitude")
    SwingRenderer(plot4K, 800, 800, true)
    spark.stop()
}
