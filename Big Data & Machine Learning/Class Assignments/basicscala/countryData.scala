package basicscala

import scala.io.Source

import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.ArrayIntToDoubleSeries
import swiftvis2.plotting.ArrayToDoubleSeries
import swiftvis2.plotting.DoubleToDoubleSeries
import swiftvis2.plotting.Plot
import swiftvis2.plotting.renderer.SwingRenderer

case class GlobalEducationData (location_id:Int, location_code:String, location_name:String, year:Int, age_group_id:Int, age_group_name:String, sex_id:Int, sex_name:String, metric:String, unit:String, mean:Double, upper:Double, lower:Double)

object GlobalEducationData extends App {
  def parseLine(line: String): GlobalEducationData = {
    var stringP = line
		if(line.contains("\"")){
		  val x = line.indexOf("\"")
		  val y = line.indexOf("\"", x+1)
		  val newStr = line.substring(x,y).replace(',', '\u060c')
		  stringP = line.substring(0,x).concat(newStr).concat(line.substring(y))
		}
	  val p = stringP.split(",")
		GlobalEducationData(p(0).toInt, p(1), p(2), p(3).toInt, p(4).toInt, p(5), p(6).toInt,
			p(7), p(8), p(9), p(10).toDouble, p(11).toDouble, p(12).toDouble)
	}

  val source = Source.fromFile("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/data/basicscala/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_Y2015M04D27.CSV")
	val lines = source.getLines.drop(1)
	val data = lines.map(parseLine).toArray.filter(_.metric == "Education Per Capita")
	source.close()

	val top5EPC = data.sortBy(-_.mean).take(5).foreach(println)
	println("=============================")

 val n3 = data.groupBy(_.age_group_name).mapValues{ ages => ages.groupBy(_.sex_name).mapValues{ sexes =>
   val locs = sexes.groupBy(_.location_name).mapValues{ locs =>
    val mean2015 = locs.filter(_.year == 2015).map(_.mean).max
    val mean1970 = locs.filter(_.year == 1970).map(_.mean).max
    mean2015 - mean1970
    }
   val greatestEntry = locs.toArray.sortBy(-_._2).take(1)
   val greatestLoc = greatestEntry(0)._1
   greatestLoc
 }
  }
 println(n3.toString())
 println

//#7
 val countryGrad = ColorGradient(80.0 -> BlueARGB, 163.0 -> GreenARGB, 102.0 -> RedARGB)
 val female25to34 = data.filter(data => data.sex_name == "Females" && data.age_group_name == "25 to 34" && (data.location_id == 163 || data.location_id == 80 || data.location_id == 102))
 var countryMap = female25to34.map(_.location_id.toDouble)
 val yearMap = data.filter(data => data.sex_name == "Females" && data.age_group_name == "25 to 34" && (data.location_id == 163)).map(_.year)
 val eduAttainOfYoungFemales = Plot.scatterPlot(yearMap, female25to34.map(_.mean), "France, US, America", "Year", "Education Attainmenet: Females ages 25-34", symbolSize = 8, symbolColor = countryGrad(countryMap))
 SwingRenderer(eduAttainOfYoungFemales, 800, 800, true)

//#9
 val france1970GDP = 20058.6816797054
 val usa1970GDP = 23309.6209459064
 val india1970GDP = 365.05738258988
 val gdps = Array(france1970GDP, usa1970GDP, india1970GDP)
 val nineAPlot = Plot.scatterPlot(gdps, female25to34.map(_.mean), "1970", "GDP", "Education Attainmenet: Females ages 25-34", symbolSize = 80, symbolColor = countryGrad(countryMap))
 SwingRenderer(nineAPlot, 800, 800, true)

 val male25to34 = data.filter(data => data.sex_name == "Males" && data.age_group_name == "25 to 34" && (data.location_id == 163 || data.location_id == 80 || data.location_id == 102))
 countryMap = male25to34.map(_.location_id.toDouble)
 val nineBPlot = Plot.scatterPlot(gdps, male25to34.map(_.mean), "1970", "GDP", "Education Attainmenet: Males ages 25-34", symbolSize = 80, symbolColor = countryGrad(countryMap))
 SwingRenderer(nineBPlot, 800, 800, true)

 val france2015GDP = 41642.3104260915
 val usa2015GDP = 51933.4048064982
 val india2015GDP = 1758.83777674579
 val gdps2015 = Array(france2015GDP, usa2015GDP, india2015GDP)
 countryMap = female25to34.map(_.location_id.toDouble)
 val nineCPlot = Plot.scatterPlot(gdps2015, female25to34.map(_.mean), "2015", "GDP", "Education Attainmenet: Females ages 25-34", symbolSize = 80, symbolColor = countryGrad(countryMap))
 SwingRenderer(nineCPlot, 800, 800, true)

 countryMap = male25to34.map(_.location_id.toDouble)
 val nineDPlot = Plot.scatterPlot(gdps2015, male25to34.map(_.mean), "2015", "GDP", "Education Attainmenet: Males ages 25-34", symbolSize = 80, symbolColor = countryGrad(countryMap))
 SwingRenderer(nineDPlot, 800, 800, true)

 //#10
 val franceGDP = Array("12991.2677157891","13524.4492898749","14224.481981772","14775.0591562818","15532.6347258984","16087.9229861581","16764.2319210598","17409.9659363379","18023.8108375959","19148.5087760733","20058.6816797054","20951.6528120921","21705.1608504203","22866.7209449323","23652.7613411661","23261.1280785167","24136.1347756493","24867.5019495597","25770.5756498831","26599.3766427339","26919.1943109963","27089.3661778818","27633.2437984653","27832.1386790419","28099.0714163019","28391.8294326453","28887.9793413264","29452.1406977034","30657.6459435035","31801.8743779472","32543.9424217428","32855.9959897504","33216.0089959065","32869.7789732528","33515.7135122261","34091.167447884","34442.184394041","35122.7462838501","36237.9630044732","37280.3410691582","38460.6821008145","38928.03030647","39078.198702169","39120.1961408612","39915.2645417897","40252.4182186225","40922.0832260575","41630.09373701","41478.9374058427","40052.3051204039","40638.33400426","41283.1481006903","41158.8849843807","41183.510959426","41374.7612490442","41642.3104260915","41968.9815739992","42567.7436477565").map(d => d.toDouble)
 val usaGDP = Array("17036.8851695882","17142.1937673897","17910.2787901684","18431.1584040472","19231.1718590604","20207.7495376936","21274.1354890107","21569.8356088859","22380.6067673191","22850.010833783","23309.6209459064","23775.2769229658","24760.1453772345","25908.9128017215","25540.5010030208","25239.919905729","26347.809281996","27286.2515144911","28500.2404573534","29082.5937779654","28734.3992597646","29191.9994879416","28362.4946163409","29406.2574686046","31268.9756446518","32306.8330567744","33133.6954441913","33975.6547953064","35083.9690428182","36033.3302026992","36312.4141825874","35803.8684213439","36566.1737698527","37078.04968394","38104.9724675631","38677.7150883663","39681.5198579033","40965.8466450522","42292.8912011426","43768.8849928326","45055.817918284","45047.4871976844","45428.6456781274","46304.0360895612","47614.2798621765","48755.6160606735","49575.401013591","49979.5338429195","49364.6445500336","47575.608562749","48375.4069462972","48786.4549755253","49498.3909155226","49971.9513571108","50871.674083306","51933.4048064982","52319.1633505427","53128.5396999252").map(d => d.toDouble)
 val indiaGDP = Array("304.231892517096","309.353562155831","312.052331884607","324.056200793601","341.096019495621","325.287386096864","318.415676162619","336.236456684756","340.361685434","354.894028472623","365.05738258988","362.767725027101","352.550055840146","355.788210052561","351.708069435998","375.083372834559","372.642613394855","390.636679931299","403.633544446174","373.832253331318","389.926284393755","403.878042963083","408.324880026208","428.069168108306","434.366504619311","446.999768550491","458.086674715335","465.982728535711","500.01327734676","518.698715936097","536.162785952453","530.894738000626","548.895783828094","563.749687660707","589.708787590248","622.303683083488","656.697143960797","670.61012159443","699.068854719027","747.252035650833","762.313340806374","785.344628061723","801.507932685869","850.293264901313","902.905794350882","971.229760739349","1044.89394043832","1130.09007066414","1156.9325271372","1237.33978594507","1345.77015320926","1416.4033912875","1474.96767422238","1550.14222964317","1645.3261405822","1758.83777674579","1862.43044438379","1963.54632112944").map(d => d.toDouble)
}
