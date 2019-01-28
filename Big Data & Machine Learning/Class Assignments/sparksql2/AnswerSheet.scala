// package sparksql2
//
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.Column
// import org.apache.spark.sql._
// import swiftvis2.plotting._
// import swiftvis2.plotting.renderer.SwingRenderer
// import swiftvis2.spark._
// import swiftvis2.plotting.styles.HistogramStyle._
// import scala.collection.mutable._
//
//
// case class VotingData(votes_id: Int, votes_dem: Double, votes_gop: Double,
//                       total_votes: Double, per_dem: Double, per_gop: Double,
//                       diff: Int, per_point_diff: Double, state_abbr: String,
//                       county_name: String, combined_fips: Int);
// case class VotingDataString(votes_id: Int, votes_dem: Double, votes_gop: Double,
//                       total_votes: Double, per_dem: Double, per_gop: Double,
//                       diff: String, per_point_diff: String, state_abbr: String,
//                       county_name: String, combined_fips: Int);
// case class ZipCodeData(zip_code: Int, latitude: Double, longitude: Double,
//                           city: String, state: String, county: String);
// case class AreaData(area_type_code: String, area_code: String, area_text: String,
//                     display_level: Int, selectable: String, sort_sequence: String);
// case class AllStatesUD(series_id: String, year: Int, period: String, value: Double, ftn: String);
//
// object AnswerSheet extends App {
//   val spark = SparkSession.builder().master("local[*]").appName("LocalAreaUnemployment").getOrCreate()
//   import spark.implicits._
//   spark.sparkContext.setLogLevel("WARN")
//
//   val vd = spark.read.schema(Encoders.product[VotingDataString].schema).
//             option("header", "true").
//             csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql2/data/2016_US_County_Level_Presidential_Results.csv").
//             as[VotingDataString]
//
//   val zipCodesStates = spark.read.schema(Encoders.product[ZipCodeData].schema).
//             option("header","true").
//             csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/zip_codes_states.csv").
//             as[ZipCodeData]
//   val areas = spark.read.
//             option("header","true").schema(Encoders.product[AreaData].schema).
//             option("delimiter", "\t").
//             csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.area").
//             as[AreaData]
//   val allStates = spark.read.schema(Encoders.product[AllStatesUD].schema).
//             option("header","true").
//             option("delimiter", "\t").
//             csv("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/sparksql/data/la/la.data.concatenatedStateFiles").
//             as[AllStatesUD]
//
//
//   def commaRemover(s: String): Int = {s.replaceAll(",", "").toInt}
//   def percentRemover(s: String): Double = {s.replace("%", "").toDouble}
//   val commaRemoverUDF = udf[Int, String] (commaRemover)
//   val percentRemoverUDF = udf[Double, String] (percentRemover)
//
//   val voteData = vd.withColumn("diff", commaRemoverUDF($"diff")).
//                     withColumn("per_point_diff", percentRemoverUDF($"per_point_diff")).
//                     filter($"state_abbr" =!= "AK")
//   val voteDataSet = vd.map{ d =>
//                       VotingData(d.votes_id, d.votes_dem, d.votes_gop,
//                                  d.total_votes, d.per_dem, d.per_gop,
//                                   commaRemover(d.diff), percentRemover(d.per_point_diff), d.state_abbr,
//                                   d.county_name, d.combined_fips)
//                     }
//
//   voteData.cache()
//   //1. What fraction of counties had a Republican majority?
//   val voteDataCount = voteData.count
//   val gopMajorities = voteData.filter($"per_gop" > 0.5)
//   println(gopMajorities.count.toDouble/voteDataCount)
//   /*0.843508997429306*/
//
//   //2. What fraction of counties went Republican by a margin of 10% or more? What about Democratic
//   val gopMargins = voteData.filter($"votes_dem" < $"votes_gop" && $"diff" >= 10)
//   println(gopMargins.count.toDouble/voteDataCount)
//   /*0.842866323907455*/
//   val demMargins = voteData.filter($"votes_dem" > $"votes_gop" && $"diff" >= 10)
//   println(demMargins.count.toDouble/voteDataCount)
//   /*0.1564910025706941*/
//
//   //3. Plot the election results with an X-axis of the number of votes cast and a Y-axis of the percent Democratic votes minus the percent Republican votes.
//
//   val totalVotes = voteData.select($"total_votes").map(_.getDouble(0)).collect()
//   val demDiff = voteData.select($"per_dem" - $"per_gop").map(_.getDouble(0)).collect()
//   val diffPlot = Plot.scatterPlot(totalVotes, demDiff, "", "Total Votes", "Percentage Difference")
//   SwingRenderer(diffPlot, 800, 800, true)
//
//   //4. Using both the election results and the zip code data, plot the election results by county geographically.
//   //So X is longitude, Y is latitude, and the color is a based on percent Democratic with 40% or lower being solid
//   //red and 60% or higher being solid blue.
//   val filteredStatesData = zipCodesStates.filter($"state" =!= "PR" && $"state" =!= "VI" && $"state" =!= "HI" && $"state" =!= "AK")
//   val geoGraphData = voteData.join(filteredStatesData, $"county_name".contains($"county") && $"state" === $"state_abbr").
//                               filter($"longitude".isNotNull && $"latitude".isNotNull).
//                               select($"longitude", $"latitude", $"per_dem")
//   val lats = geoGraphData.select($"latitude").map(_.getDouble(0)).collect()
//   val longs = geoGraphData.select($"longitude").map(_.getDouble(0)).collect()
//   val per = geoGraphData.select($"per_dem").map(_.getDouble(0)).collect()
//   val cg = ColorGradient(0.4 -> RedARGB, 0.6 -> BlueARGB)
//   val geoPlot = Plot.scatterPlots(Seq((longs, lats, per.map(cg), 5)),"", "Longitude", "Latitude")
//   SwingRenderer(geoPlot, 800, 800, true)
//   voteData.unpersist()
//
//   5
//
//   //BDF
//   //6/1990, 3/1991, 2/2001, 11/2001, 11/2007, 6/2009
//   val allUR = allStates.filter($"series_id".substr(19,2) === "03").select($"value", $"period", $"series_id".substr(4,15).as("area"))
//   val uR0690 = allUR.filter($"year" === 1990 && $"period" === "M06").select($"value", $"area")
//   val uR0391 = allUR.filter($"year" === 1991 && $"period" === "M03").select($"value", $"area")
//   val uR0201 = allUR.filter($"year" === 2001 && $"period" === "M02").select($"value", $"area")
//   val uR1101 = allUR.filter($"year" === 2001 && $"period" === "M11").select($"value", $"area")
//   val uR1107 = allUR.filter($"year" === 2007 && $"period" === "M11").select($"value", $"area")
//   val uR0609 = allUR.filter($"year" === 2009 && $"period" === "M06").select($"value", $"area")
//   val metroAreas = areas.filter($"area_type_code" === "B")
//   val microAreas = areas.filter($"area_type_code" === "D")
//   val countyAreas = areas.filter($"area_type_code" === "F")
//
//   val months = Array(uR0690, uR0391, uR0201, uR1101, uR1107, uR0609)
//   val differentAreas = Array(metroAreas, microAreas, countyAreas)
//   var seqArr:Array[Array[(Array[Double], Int)]] = Array.fill[Array[(Array[Double],Int)]](3)(Array.fill[(Array[Double], Int)](6)((Array.empty,0)))
//   var x,y = 0
//   var color = GreenARGB
//   println(seqArr.length)
//   println(seqArr(0).length)
//   for(x <- 0 until differentAreas.length)
//   {
//     for(y <- 0 until months.length)
//     {
//       color = if (y % 2 == 0) GreenARGB else RedARGB
//       var r = months(y).join(differentAreas(x), $"area" === $"area_code").select($"value")
//     //  r.show()
//       var t = (r.map(_.getDouble(0)).collect(), color)
//       seqArr(x)(y) = t
//     }
//   }
//
//   val bins = (0.0 to 50.0 by 1.0).toArray
//   var test = metroAreas.join(uR0690, $"area" === $"area_code").select($"value").map(_.getDouble(0)).collect()
//   val histPlot = Plot.histogramGrid(bins, Seq(Seq(DataAndColor(seqArr(0)(0)._1, seqArr(0)(0)._2), DataAndColor(seqArr(0)(1)._1, seqArr(0)(1)._2), DataAndColor(seqArr(0)(2)._1, seqArr(0)(2)._2), DataAndColor(seqArr(0)(3)._1, seqArr(0)(3)._2), DataAndColor(seqArr(0)(4)._1, seqArr(0)(4)._2), DataAndColor(seqArr(0)(5)._1, seqArr(0)(5)._2)),
//                                               Seq(DataAndColor(seqArr(1)(0)._1, seqArr(1)(0)._2), DataAndColor(seqArr(1)(1)._1, seqArr(1)(1)._2), DataAndColor(seqArr(1)(2)._1, seqArr(1)(2)._2), DataAndColor(seqArr(1)(3)._1, seqArr(1)(3)._2), DataAndColor(seqArr(1)(4)._1, seqArr(1)(4)._2), DataAndColor(seqArr(1)(5)._1, seqArr(1)(5)._2)),
//                                               Seq(DataAndColor(seqArr(2)(0)._1, seqArr(2)(0)._2), DataAndColor(seqArr(2)(1)._1, seqArr(2)(1)._2), DataAndColor(seqArr(2)(2)._1, seqArr(2)(2)._2), DataAndColor(seqArr(2)(3)._1, seqArr(2)(3)._2), DataAndColor(seqArr(2)(4)._1, seqArr(2)(4)._2), DataAndColor(seqArr(2)(5)._1, seqArr(2)(5)._2)))
//                                         , true, true, "", "Unemployment Rate", "Count")
//   val histPlot = Plot.histogramPlot(bins, seqArr(0), false, false, "j", "ds", binsOnX = false)
//   SwingRenderer(histPlot, 800, 800, true)
//
//   //6. I am interested in correlations between employment status and voting tendencies. Let's look at this in a few different ways.
//   //a. For all counties, calculate the correlation coefficient between the unemployment rate and percent democratic vote for November 2016.
//   //(Note that this will be a single number.) What does that number imply?
//   case class SeriesByCountyName(area_code: String, value: Double, area_text: String);
//   case class SeriesWithVoteData(area_code: String, value: Double, per_dem: Double); //per_gop: Double, total_votes: Double);
//   case class RatePopParty(rate: Double, pop: Double, dem_per: Double, area_code: String);
//
//   val countyCodes = areas.filter($"area_type_code" === "F")
//   val allCountyUR = allStates.filter($"series_id".substr(19,2) === "03" && $"year" === 2016 && $"period" === "M11").
//                     joinWith(countyCodes, $"series_id".substr(4,15) === $"area_code").
//                     map(j => SeriesByCountyName(j._1.series_id.substring(3,18), j._1.value, j._2.area_text))
//   val uRData = allCountyUR.joinWith(voteDataSet, $"area_text".contains($"county_name") && $"area_text".contains($"state_abbr")).map(j => SeriesWithVoteData(j._1.area_code, j._1.value, j._2.per_dem))
//   uRData.select(corr($"value", $"per_dem")).show()
//   //0.05764283229769167
//   //Better: 0.16672925713740175
//
//   //b. Make a scatter plot that you feel effectively shows the three values of population, party vote, and the unemployment rate (again in November 2016). For the population,
//   //you can use the labor force or the number of votes cast. Your scatter plot should have one point per county. In addition to X and Y, you can use size and color. What
//   //does your plot show about these three values?
//   val allCountyPop = allStates.filter($"series_id".substr(19,2) === "06" && $"year" === 2016 && $"period" === "M11").
//                     joinWith(countyCodes, $"series_id".substr(4,15) === $"area_code").
//                     map(j => SeriesByCountyName(j._1.series_id.substring(3,18), j._1.value, j._2.area_text))
//
//   val popData = allCountyPop.joinWith(voteDataSet, $"area_text".contains($"county_name")  && $"area_text".contains($"state_abbr")).map(d => SeriesWithVoteData(d._1.area_code, d._1.value, d._2.per_dem))
//   val joinData = uRData.joinWith(popData, popData("area_code") === uRData("area_code")).map(d => RatePopParty(d._1.value, d._2.value, d._1.per_dem, d._1.area_code))
//   val rates = joinData.map(_.rate).collect()
//   val pops = joinData.map(_.pop).collect()
//   val demPrcs = joinData.map(_.dem_per).collect()
//   val cg2 = ColorGradient(0.4 -> RedARGB, 0.6 -> BlueARGB)
//   val rppPlot = Plot.scatterPlots(Seq((rates, pops, demPrcs.map(cg2), 10)),"", "Unemployment Rate", "Population")
//   SwingRenderer(rppPlot, 800, 800, true)
//
//   //7. Look at the relationships between voter turnout (approximate by votes/labor force), unemployment, and
//   //political leaning. Are there any significant correlations between these values? If so, what are they? You can
//   //use plots or statistical measures to draw and illustrate your conclusions.
//   case class TurnEmployParty(turnout: Double, unemployment: Double, per_dem: Double, area_code: String);
//
//   val allCountyUnemploy = allStates.filter($"series_id".substr(19,2) === "04" && $"year" === 2016 && $"period" === "M11").
//                           joinWith(countyCodes, $"series_id".substr(4,15) === $"area_code").
//                           map(j => SeriesByCountyName(j._1.series_id.substring(3,18), j._1.value, j._2.area_text))
//   val unemployData = allCountyUnemploy.joinWith(voteDataSet, $"area_text".contains($"county_name") && $"area_text".contains($"state_abbr")).map(j => SeriesWithVoteData(j._1.area_code, j._1.value, j._2.per_dem))
//   val turnoutData = allCountyPop.joinWith(voteDataSet, $"area_text".contains($"county_name")  && $"area_text".contains($"state_abbr")).map(d => SeriesWithVoteData(d._1.area_code, d._2.total_votes/d._1.value, d._2.per_dem))
//   val joinData2 = unemployData.joinWith(turnoutData, turnoutData("area_code") === unemployData("area_code")).map(d => TurnEmployParty(d._2.value, d._1.value, d._1.per_dem, d._1.area_code))
//   joinData2.show()
//
//   val unemploymnt = joinData2.map(_.unemployment).collect()
//   val turnout = joinData2.map(_.turnout).filter(_ < 6).collect()
//   val demPrcs2 = joinData2.map(_.per_dem).collect()
//   val tepPlot = Plot.scatterPlots(Seq((turnout, unemploymnt, demPrcs2.map(cg2), 10)),"", "Voter Turnout", "Unemployment")
//   SwingRenderer(tepPlot, 800, 800, true)
//   val cgu = ColorGradient(0.4 -> RedARGB, 0.6 -> BlueARGB)
//
//
//   spark.sparkContext.stop()
// }
