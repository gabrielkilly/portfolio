package basicscala

import scala.io.Source

import swiftvis2.plotting
import swiftvis2.plotting._
import swiftvis2.plotting.ArrayIntToDoubleSeries
import swiftvis2.plotting.ArrayToDoubleSeries
import swiftvis2.plotting.DoubleToDoubleSeries
import swiftvis2.plotting.Plot
import swiftvis2.plotting.renderer.SwingRenderer

case class GDPData (country_name:String, country_code:String,indicator_name:String, indicator_code:String, yr1960:Double, yr1961:Double, yr1962:Double, yr1963:Double, yr1964:Double, yr1965:Double, yr1966:Double,yr1967:Double, yr1968:Double, yr1969:Double, yr1970:Double, yr1971:Double, yr1972:Double, yr1973:Double, yr1974:Double, yr1975:Double, yr1976:Double, yr1977:Double, yr1978:Double, yr1979:Double, yr1980:Double, yr1981:Double, yr1982:Double, yr1983:Double, yr1984:Double, yr1985:Double, yr1986:Double, yr1987:Double, yr1988:Double, yr1989:Double, yr1990:Double, yr1991:Double, yr1992:Double, yr1993:Double, yr1994:Double, yr1995:Double, yr1996:Double, yr1997:Double, yr1998:Double, yr1999:Double, yr2000:Double, yr2001:Double, yr2002:Double, yr2003:Double, yr2004:Double, yr2005:Double, yr2006:Double, yr2007:Double, yr2008:Double, yr2009:Double, yr2010:Double,yr2011:Double, yr2012:Double, yr2013:Double, yr2014:Double, yr2015:Double, yr2016:Double, yr2017:Double)
object GDPData extends App {
  def commaRemover(line:String): (String, String) = {
    var end = line.substring(1).indexOf('\"')+1
    var newStr = line.substring(0,end + 1).replace(',', '\u060c') ++ ","
    if(newStr.length == line.length+1)
      return (newStr, "")
    return (newStr, line.substring(end + 2))
  }
  def recurParser(prev:Char, line: String): String = {
    if(line.length <= 1)
    {
      return line
    }
    else if (line(0) == '\"')
    {
      var tup = commaRemover(line)
      var newStr = tup._1
      var restOfStr = tup._2
      return (newStr)++ recurParser(' ', restOfStr)
    }
    else if(prev == ',' && line(0).toChar == ',')
      return " ,"++(recurParser(line(0), line.substring(1)))
    else
      return line(0).toString++(recurParser(line(0), line.substring(1)))
  }
  def parseLine(line: String): GDPData = {
    val p = recurParser(',', line).split(',')

    val yrArray = Array.fill[Double](58)(0.0)
    for(i <- 0 to 57)
    {
      if(p(i + 4) != "\"\"")
        yrArray(i) = p(i + 4).substring(1, p(i + 4).length - 2).toDouble
      else
        yrArray(i) = -1.0
    }
		GDPData(p(0), p(1), p(2), p(3), yrArray(0), yrArray(1), yrArray(2),yrArray(3),yrArray(4), yrArray(5), yrArray(6), yrArray(7), yrArray(8), yrArray(9), yrArray(10), yrArray(11), yrArray(12), yrArray(13), yrArray(14), yrArray(15), yrArray(16), yrArray(17), yrArray(18), yrArray(19), yrArray(20), yrArray(21), yrArray(22), yrArray(23), yrArray(24), yrArray(25), yrArray(26), yrArray(27), yrArray(28), yrArray(29), yrArray(30), yrArray(31), yrArray(32), yrArray(33), yrArray(34), yrArray(35), yrArray(36), yrArray(37), yrArray(38), yrArray(39), yrArray(40), yrArray(41), yrArray(42), yrArray(43), yrArray(44), yrArray(45), yrArray(46), yrArray(47), yrArray(48), yrArray(49), yrArray(50), yrArray(51), yrArray(52), yrArray(53), yrArray(54), yrArray(55), yrArray(56), yrArray(57))
	}
  val source = Source.fromFile("/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/data/basicscala/API_NY.GDP.PCAP.KD_DS2_en_csv_v2_10081022/API_NY.GDP.PCAP.KD_DS2_en_csv_v2_10081022.csv")
  val lines = source.getLines.drop(5)
  val data = lines.map(parseLine).toArray
  source.close()

  var largest1970GDP = data.sortBy(-_.yr1970).take(1).foreach(println)
  println
  var smallest1970GDP = data.filter(_.yr1970 > -1).sortBy(_.yr1970).take(1).foreach(println)
  println
  var largest2015GDP = data.sortBy(-_.yr2015).take(1).foreach(println)
  println
  var smallest2015GDP = data.filter(_.yr2015 > -1).sortBy(_.yr2015).take(1).foreach(println)
  println
  var GDPDiffByCountry = data.groupBy(_.country_name).mapValues { countries =>
    var GDP2015 = countries.map(_.yr2015).max
    var GDP1970 = countries.map(_.yr1970).max
    GDP2015 - GDP1970
  }
  var maxGDPDiffArray = GDPDiffByCountry.toArray.sortBy(-_._2).take(5).foreach(println)

  //Problem #8
  var gdps = Array("304.231892517096","309.353562155831","312.052331884607","324.056200793601","341.096019495621","325.287386096864","318.415676162619","336.236456684756","340.361685434","354.894028472623","365.05738258988","362.767725027101","352.550055840146","355.788210052561","351.708069435998","375.083372834559","372.642613394855","390.636679931299","403.633544446174","373.832253331318","389.926284393755","403.878042963083","408.324880026208","428.069168108306","434.366504619311","446.999768550491","458.086674715335","465.982728535711","500.01327734676","518.698715936097","536.162785952453","530.894738000626","548.895783828094","563.749687660707","589.708787590248","622.303683083488","656.697143960797","670.61012159443","699.068854719027","747.252035650833","762.313340806374","785.344628061723","801.507932685869","850.293264901313","902.905794350882","971.229760739349","1044.89394043832","1130.09007066414","1156.9325271372","1237.33978594507","1345.77015320926","1416.4033912875","1474.96767422238","1550.14222964317","1645.3261405822","1758.83777674579","1862.43044438379","1963.54632112944").map(d => d.toDouble)
  var country_codes = Array(80, 16)
  var yrs = Array(1960,1961,1962,1963,1964,1965,1966,1967,1968,1969,1970,1971,1972,1973,1974,1975,1976,1977,1978,1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017)
  val gdpPlot = Plot.scatterPlot(yrs, gdps, "India", "Year", "GDP", symbolSize = 8)
  SwingRenderer(gdpPlot, 800, 800, true)
}
