package sparkrdd2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import sparkrdd._
import scala.math._

object AnswerSheet extends App {
  val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  var lines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv")
  val tempData2017 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }

  lines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/1897.csv")
  val tempData1897 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }

  lines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-stations.txt")
  val stationData = lines.map { line =>
    StationData.parseLine(line)
  }


//Number 1

val checkFlaw1 = tempData2017.filter(td => (td.element == "TMIN" || td.element == "TMAX") && td.date == 20170719 && td.id == "USS0047P03S" ) //making sure values were calculated correctly
val checkFlaw2 = tempData2017.filter(td => (td.element == "TMIN" || td.element == "TMAX") && td.date == 20170522 && td.id == "USR0000APED" )
val checkFlaw3 = tempData2017.filter(td => (td.element == "TMIN" || td.element == "TMAX" && td.id == "USS0045R01S0" && td.value > 800))
checkFlaw1.foreach(println)
checkFlaw2.foreach(println)
println(checkFlaw3.count)

val diffByIDAndDate = tempData2017.filter(td => (td.element == "TMIN" || td.element == "TMAX") && td.value > -999).map(td => ((td.id, td.date), td.value)).reduceByKey((acc, v) => (acc - v)).map(d => (d._1, abs(d._2)))
val highestDiffDay2017 = diffByIDAndDate.map(_.swap).sortByKey(false)
highestDiffDay2017.take(1).foreach(println)

//Number 2 TODO USE AVERAGES INSTEAD SUM
//val highestDiffStation2017 = diffByIDAndDate.map(d => (d._1._1, d._2)).reduceByKey((acc, v) => (acc + v)).map(_.swap).sortByKey(false)
val highestDiffStation2017 = tempData2017.filter(td => td.element == "TMIN" || td.element == "TMAX" && td.value > -999).map(td => (td.id, td.value)).groupByKey().map(d => ((d._2.max - d._2.min), d._1)).sortByKey(false)
highestDiffStation2017.take(1).foreach(println)

//Number 3
val stdDevOfTMax = tempData2017.filter(sd => sd.element == "TMAX").map(_.value.toDouble).stdev()
println(stdDevOfTMax)
val stdDevOfTMin = tempData2017.filter(sd => sd.element == "TMIN").map(_.value.toDouble).stdev()
println(stdDevOfTMin)

//Number 4
val activeStationsInBothYears = tempData2017.map(_.id).intersection(tempData1897.map(_.id)).count
println(activeStationsInBothYears)

//Number 5
val filteredTempData = tempData2017.filter(td => td.element == "TMAX" || td.element == "TMIN")
val lowStationGroup = stationData.filter(_.latitude < 35).map(sd => (sd.id, sd.id))
val lowLatGroup = filteredTempData.map(td => (td.id, td)).join(lowStationGroup).map(_._2._1)
val midLatGroup = filteredTempData.map(td => (td.id, td)).join(stationData.filter(sd => sd.latitude > 35 && sd.latitude <= 42).map(sd => (sd.id, sd.id))).map(_._2._1)
val highLatGroup = filteredTempData.map(td => (td.id, td)).join(stationData.filter(_.latitude > 42).map(sd => (sd.id, sd.id))).map(_._2._1)
//5a
val stdTMAXLow = lowLatGroup.filter(_.element == "TMAX").map(_.value).stdev()
val stdTMAXMid = midLatGroup.filter(_.element == "TMAX").map(_.value.toDouble).stdev()
val stdTMAXHigh = highLatGroup.filter(_.element == "TMAX").map(_.value.toDouble).stdev()

val stdTMINLow = lowLatGroup.filter(_.element == "TMIN").map(_.value).stdev()
val stdTMINMid = midLatGroup.filter(_.element == "TMIN").map(_.value.toDouble).stdev()
val stdTMINHigh = highLatGroup.filter(_.element == "TMIN").map(_.value.toDouble).stdev()

println("Low: TMAX deviation = " + stdTMAXLow + " TMIN deviation = " + stdTMINLow)
println("Mid: TMAX deviation = " + stdTMAXMid + " TMIN deviation = " + stdTMINMid)
println("High: TMAX deviation = " + stdTMAXHigh + " TMIN deviation = " + stdTMINHigh)

//5b
val stdAvgDailyLow = lowLatGroup.map(llg => ((llg.id, llg.date), llg.value)).reduceByKey((acc, v) => (acc + v)).map(_._2/2).stdev()
val stdAvgDailyMid = midLatGroup.map(llg => ((llg.id, llg.date), llg.value)).reduceByKey((acc, v) => (acc + v)).map(_._2/2).stdev()
val stdAvgDailyHigh = highLatGroup.map(llg => ((llg.id, llg.date), llg.value)).reduceByKey((acc, v) => (acc + v)).map(_._2/2).stdev()

println("Low: Avg Daily Deviation = " + stdAvgDailyLow)
println("Mid: Avg Daily Deviation = " + stdAvgDailyMid)
println("High: Avg Daily Deviation = " + stdAvgDailyHigh)

//5c
var temps = lowLatGroup.filter(_.element == "TMAX").map(_.value/10).collect()
var bins = temps.min to temps.max by (temps.max - temps.min)/20
var highTempHistLow = Plot.histogramPlotFromData(bins, temps, GreenARGB, "< 35\u00b0 Latitude", "Temperature \u00b0C", "Number of Stations")
SwingRenderer(highTempHistLow, 800, 800, true)

temps = midLatGroup.filter(_.element == "TMAX").map(_.value/10).collect()
bins = temps.min to temps.max by (temps.max - temps.min)/20
var highTempHistMid = Plot.histogramPlotFromData(bins, temps, GreenARGB, "> 35\u00b0 And < 42\u00b0 Latitude", "Temperature \u00b0C", "Number of Stations")
SwingRenderer(highTempHistMid, 800, 800, true)

temps = highLatGroup.filter(_.element == "TMAX").map(_.value/10).collect()
bins = temps.min to temps.max by (temps.max - temps.min)/20
var highTempHistHigh = Plot.histogramPlotFromData(bins, temps, GreenARGB, "> 42\u00b0 Latitude", "Temperature \u00b0C", "Number of Stations")
SwingRenderer(highTempHistHigh, 800, 800, true)

//Number 6
val keyedByID = tempData2017.filter(_.element == "TMAX").map(td => (td.id, td.value))
val avgTempByStation= keyedByID.aggregateByKey(0.0 -> 0)({ case ((sum, cnt), v) =>
  (sum+v, cnt+1)
}, { case ((s1, c1), (s2, c2)) => (s1+s2, c1+c2) }).map(tup => tup._2._1/tup._2._2).collect
val latsAndLongs = stationData.map(sd => (sd.id, (sd.latitude, sd.longitude))).join(tempData2017.filter(_.element == "TMAX").map(td => (td.id, td.id)).distinct).map(_._2._1)
val longs = latsAndLongs.map(_._2).distinct.collect()
val lats = latsAndLongs.map(_._1).distinct.collect()
val tempColors = ColorGradient(100.0 -> RedARGB, 50.0 -> GreenARGB, 0.0 -> BlueARGB)
val plotAvgTemp = Plot.scatterPlot(longs, lats, "Stations Average High Temperature", "Longitude", "Latitude", symbolSize = 8, symbolColor = tempColors(avgTempByStation))
SwingRenderer(plotAvgTemp, 800, 800, true)

//Number 7
  lines = sc.textFile("/data/BigData/ghcn-daily/1907.csv")
  val tempData1907 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1917.csv")
  val tempData1917 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1927.csv")
  val tempData1927 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1937.csv")
  val tempData1937 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1947.csv")
  val tempData1947 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1957.csv")
  val tempData1957 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
 }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1967.csv")
  val tempData1967 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1977.csv")
  val tempData1977 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1987.csv")
  val tempData1987 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/1997.csv")
  val tempData1997 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

  lines = sc.textFile("/data/BigData/ghcn-daily/2007.csv")
  val tempData2007 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }.filter(td => td.element == "TMAX" || td.element == "TMIN")

//7a
val filteredTD1897 = tempData1897.filter(td => td.element == "TMAX" || td.element == "TMIN")
val mappedTD1897 = filteredTD1897.map(_.value)
val filteredTD2017 = tempData2017.filter(td => td.element == "TMAX" || td.element == "TMIN")
val mappedTD2017 = filteredTD2017.map(_.value)
val avgTemp1897 = mappedTD1897.sum/mappedTD1897.count.toDouble
val avgTemp2017 = mappedTD2017.sum/mappedTD2017.count.toDouble
println("Avg1897: " + avgTemp1897 + ", Avg2017: " + avgTemp2017 + " , Difference: " + (avgTemp2017-avgTemp1897))

//7b
val aggregate1897 = filteredTD1897.map(td => (td.id, td.value)).aggregateByKey(0.0 -> 0)({ case ((sum, cnt), v) =>
  (sum+v, cnt+1)
}, { case ((s1, c1), (s2, c2)) => (s1+s2, c1+c2) })
val aggregate2017 = filteredTD2017.map(td => (td.id, td.value)).aggregateByKey(0.0 -> 0)({ case ((sum, cnt), v) =>
  (sum+v, cnt+1)
}, { case ((s1, c1), (s2, c2)) => (s1+s2, c1+c2) })
val joinedSumAndCount = aggregate1897.join(aggregate2017).map{ case (id,((s1, c1), (s2, c2))) => ((s1+s2), (c1+c2)) }
val joinedSum = joinedSumAndCount.map(_._1).sum.toDouble
val joinedCount = joinedSumAndCount.map(_._2).sum.toDouble
val joinedAvg = joinedSum/joinedCount
println("Avg Of Intersecting Stations: " + joinedAvg)

//7c
val td1907Avg = tempData1907.map(_.value).sum/tempData1907.count.toDouble
val td1917Avg = tempData1917.map(_.value).sum/tempData1917.count.toDouble
val td1927Avg = tempData1927.map(_.value).sum/tempData1927.count.toDouble
val td1937Avg = tempData1937.map(_.value).sum/tempData1937.count.toDouble
val td1947Avg = tempData1947.map(_.value).sum/tempData1947.count.toDouble
val td1957Avg = tempData1957.map(_.value).sum/tempData1957.count.toDouble
val td1967Avg = tempData1967.map(_.value).sum/tempData1967.count.toDouble
val td1977Avg = tempData1977.map(_.value).sum/tempData1977.count.toDouble
val td1987Avg = tempData1987.map(_.value).sum/tempData1987.count.toDouble
val td1997Avg = tempData1997.map(_.value).sum/tempData1997.count.toDouble
val td2007Avg = tempData2007.map(_.value).sum/tempData2007.count.toDouble

val avgs = Array(avgTemp1897,
	               td1907Avg,
		             td1917Avg,
                 td1927Avg,
                 td1937Avg,
                 td1947Avg,
                 td1957Avg,
                 td1967Avg,
                 td1977Avg,
                 td1987Avg,
                 td1997Avg,
                 td2007Avg,
                 avgTemp2017)

val years = Array(1897,1907.1917,1927,1937,1947,1957,1967,1977,1987,1997,2007,2017)
val aPlot = Plot.scatterPlot(years, avgs, "Averages of Approach A", "Years", "Averages")
SwingRenderer(aPlot,800,800)

//7d

}
