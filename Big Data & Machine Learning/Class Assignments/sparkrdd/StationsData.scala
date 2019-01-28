package sparkrdd

import scala.io.Source

case class StationData(id: String, latitude: Double, longitude: Double, elevation: Double, name: String);

object StationData extends App {
  def getLeftovers(e:Array[String]): String = {
    var str = e(4)
    if(4 < e.size-1) {
      for(i <- 4 to e.size-1)
      {
        str = str + e(i)
      }
    }
    return str
  }
  def parseLine(line: String): StationData = {
		val e = line.split(" ").filter(s => s.size > 1)
    StationData(e(0), e(1).toDouble, e(2).toDouble, e(3).toDouble, getLeftovers(e))
	}
}
