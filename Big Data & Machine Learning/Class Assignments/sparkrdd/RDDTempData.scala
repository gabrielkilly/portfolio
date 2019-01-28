package sparkrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.spark._
import sparkrdd._
import scala.math._

case class RDDTempData(id:String, date:Int, element:String, value:Int) // mFlag:String, qFlag:String, sFlag:String, obsTime:Int

object RDDTempData extends App {
  val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  var lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd/ghcnd-stations.txt")
  val stationData = lines.map { line =>
    StationData.parseLine(line)
  }

  lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd/2017.csv")
  val tempData2017 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }

  lines = sc.textFile("/home/gkilly/Projects/Trinity/Big-Data/in-class-code-gkilly/data/sparkrdd/2018.csv")
  val tempData2018 = lines.map { line =>
      val p = line.split(",")
      RDDTempData(p(0), p(1).toInt, p(2), p(3).toInt)
  }

  println
  println
  println
  println

  //#1
  val stationsInTexas = stationData.filter(_.name.substring(0,2) == "TX")
  println(stationsInTexas.count)

  //#2
  val activeStationsInTexas2017 = tempData2017.map(_.id).intersection(stationsInTexas.map(_.id))
  println(activeStationsInTexas2017.count)

  //#3
  val highestTemp2018 = tempData2018.filter(_.element == "TMAX").sortBy(-_.value).take(5)
  highestTemp2018.foreach(println)

  //#4
  val inactiveStations = stationData.count - tempData2017.map(_.id).distinct.count
  val inactiveStations2 = stationData.map(_.id).subtract(tempData2017.map(_.id)).count
  println(inactiveStations)
  println(inactiveStations2)

  //#5
  //  val maxRainfallInTexas2017
  val randomStation = stationData.map(_.id).max
  println(randomStation)
  val maxRainfallInTexas2017 = tempData2017.filter(d => d.id == randomStation && d.element == "PRCP").sortBy(-_.value).take(5)
  maxRainfallInTexas2017.foreach(println)

  //#6
  val maxRainfallInIndia2017 = tempData2017.filter(d => d.id.substring(0, 2) == "IN" && d.element == "PRCP").sortBy(-_.value).take(5)
  maxRainfallInIndia2017.foreach(println)

  //#7
  val stationsInSanAntonio = stationData.filter(_.name.contains("TXSANANTONIO"))
  //val stationsNotInSanAntonio = stationData.filter(s => !(s.name.contains("TXSANANTONIO")))
  println(stationsInSanAntonio.count)

  //#8
  val activeTempStationsInSanAntonio2017 = tempData2017.filter(td => td.element == "TMAX" || td.element == "TMIN").map(_.id).intersection(stationsInSanAntonio.map(_.id))
  val activeStationsInSanAntonio2017 = tempData2017.map(_.id).intersection(stationsInSanAntonio.map(_.id)).collect
  println(activeTempStationsInSanAntonio2017.count)

  //#9

  val sanAntonioStations = tempData2017.filter(d => activeStationsInSanAntonio2017.contains(d.id))

  val largestDailyIncreases = sanAntonioStations.filter(td => td.element == "TMAX" || td.element == "TMIN").groupBy(_.id).mapValues { tdById =>
    val dateWithVal = tdById.groupBy(_.date).mapValues { tdByDate =>
      val getMax = tdByDate.filter(_.element == "TMAX").map(_.value)      //take(1).value
      val getMin = tdByDate.filter(_.element == "TMIN").map(_.value)     //take(1).value
      getMax.max - getMin.min
    }
    dateWithVal.map(_._2).max
  }
  largestDailyIncreases.sortBy(-_._2).take(2).foreach(println)
  println(largestDailyIncreases.map(_._2).max)

  //#10

  val getVals = sanAntonioStations.filter(td => td.element == "TMAX" || td.element == "PRCP").groupBy(_.id).mapValues { tdById =>
    val dateSums = tdById.groupBy(_.date).mapValues { tdByDate =>
      val tmax = tdByDate.filter(_.element == "TMAX").map(_.value)
      val prcp = tdByDate.filter(_.element == "PRCP").map(_.value)
      val ansTup = if(tmax.toArray.size == 0 || prcp.toArray.size == 0) (-100,-100) else (tmax.max, prcp.max)
      ansTup
    }
    dateSums.filter(_._2._2 != -100).map(_._2)
  }.map(_._2).filter(m => m.size > 0).collect.flatten

  val n:Double = getVals.size
  val sumOfX:Double = getVals.map(_._1).sum
  val sumOfY:Double = getVals.map(_._2).sum
  val sumOfXY:Double = getVals.map(tp => tp._1 * tp._2).sum
  val sumOfXSqr:Double = getVals.map(tp => tp._1 * tp._1).sum
  val sumOfYSqr:Double = getVals.map(tp => tp._2 * tp._2).sum
  val correlationCoefficient:Double = ((n*sumOfXY) - (sumOfX*sumOfY))/sqrt((n*sumOfXSqr - sumOfXSqr) * (n*sumOfYSqr - sumOfYSqr))
  println(correlationCoefficient)

  //#11
  //Inefficent way of finding stations with decent data
  //stationData.filter(_.latitude.toInt == 74).take(10).foreach(println)
  //println(tempData2017.filter(d => d.id == "CA002401025" && d.element == "TMAX").count)
  //println(tempData2017.filter(d => d.id == "CA002401050" && d.element == "TMAX").count)
  var daysInMonth = Array(31,28,31,30,31,30,31,31,30,31,30,31)
  var stationTemp = tempData2017.filter(d => d.id == "CA003072655" && d.element == "TMAX").sortBy(d => d.date)
  var time = stationTemp.map { d =>
    val newDate = d.date.toString
    val month = newDate.substring(4,6).toInt
    println("Month: " + month)
    val day = newDate.substring(5,7).toInt
    println("Day: " + day)
    val monthToDay = daysInMonth.slice(0,month).sum
    println("MtoDay: " + monthToDay)
    println("Sum: " + day + monthToDay)
    day + monthToDay
  }.distinct.collect //stationTemp.map(_.date-20170000).collect
  var rainfall = stationTemp.map(_.value/10.0).collect
  val chipewyanPlot = Plot.scatterPlot(time, rainfall, "Fort Chipewyan", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(chipewyanPlot, 800, 800, true)

  stationTemp = tempData2017.filter(d => d.id == "USW00013838" && d.element == "TMAX" && d.value != "-").sortBy(d => d.date)
  rainfall = stationTemp.map(_.value/10.0).collect
  time = stationTemp.map { d =>
    val newDate = d.date.toString
    val month = newDate.substring(4,6).toInt
    val day = newDate.substring(5,7).toInt
    val monthToDay = daysInMonth.slice(0,month).sum
    day + monthToDay
  }.distinct.collect
  val alPlot = Plot.scatterPlot(time, rainfall, "Mobile DWNT AP, AL", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(alPlot, 800, 800, true)

  stationTemp = tempData2017.filter(d => d.id == "CHM00059855" && d.element == "TMAX" && d.value != "-").sortBy(d => d.date)
  rainfall = stationTemp.map(_.value/10.0).collect
  time = stationTemp.map { d =>
    val newDate = d.date.toString
    val month = newDate.substring(4,6).toInt
    val day = newDate.substring(5,7).toInt
    val monthToDay = daysInMonth.slice(0,month).sum
    day + monthToDay
  }.distinct.collect
  val qioPlot = Plot.scatterPlot(time, rainfall, "QIONGHAI", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(qioPlot, 800, 800, true)

  stationTemp = tempData2017.filter(d => d.id == "BN000065335" && d.element == "TMAX" && d.value != "-").sortBy(d => d.date)
  rainfall = stationTemp.map(_.value/10.0).collect
  time = stationTemp.map { d =>
    val newDate = d.date.toString
    val month = newDate.substring(4,6).toInt
    val day = newDate.substring(5,7).toInt
    val monthToDay = daysInMonth.slice(0,month).sum
    day + monthToDay
  }.distinct.collect
  val savePlot = Plot.scatterPlot(time, rainfall, "SAVE", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(savePlot, 800, 800, true)

  stationTemp = tempData2017.filter(d => d.id == "NO000099710" && d.element == "TMAX" && d.value != "-").sortBy(d => d.date)
  rainfall = stationTemp.map(_.value/10.0).collect
  time = stationTemp.map { d =>
    val newDate = d.date.toString
    val month = newDate.substring(4,6).toInt
    val day = newDate.substring(5,7).toInt
    val monthToDay = daysInMonth.slice(0,month).sum
    day + monthToDay
  }.distinct.collect
  val bjorPlot = Plot.scatterPlot(time, rainfall, "BJOERNOEYA", "Day", "Temperature (degrees Celcius)", symbolSize = 8)
  SwingRenderer(bjorPlot, 800, 800, true)

  println
  println
  println
  println
}
