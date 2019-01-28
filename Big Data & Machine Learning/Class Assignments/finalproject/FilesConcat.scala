package finalproject

import java.io._
import scala.io.Source


object FilesConcat {
  def main(args: Array[String]): Unit = {
    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
          d.listFiles.filter(_.isFile).toList
      } else {
          List[File]()
      }
    }

    val baseURL = "/home/gkilly/Projects/Trinity/Big-Data/big-data-assignments-f18-gkilly/src/main/scala/finalproject/data/"

    // //FTP Storm Data
    // val ncdcFiles = getListOfFiles(baseURL + "ftp.ncdc.noaa.gov/").map(_.toString)
    //
    // val locFiles = ncdcFiles.filter(_.contains("locations"))
    // val lfHeader = Source.fromFile(locFiles(0)).getLines.toArray.take(1).mkString("\n")
    // val lf = locFiles.map(Source.fromFile(_).getLines.toArray.drop(1).mkString("\n"))
    // val pwLoc = new PrintWriter(new File(baseURL + "storm-locations.csv"))
    // pwLoc.write(lf.mkString("\n"))
    // pwLoc.close()
    //
    // val detFiles = ncdcFiles.filter(_.contains("details"))
    // val dfHeader = Source.fromFile(detFiles(0)).getLines.toArray.take(1).mkString("\n")
    // val df = detFiles.map(Source.fromFile(_).getLines.toArray.drop(1).mkString("\n"))
    // val pwDet = new PrintWriter(new File(baseURL + "storm-details.csv"))
    // pwDet.write(df.mkString("\n"))
    // pwDet.close()

    //NOAA Weather Data
    val rrPRFiles = getListOfFiles(baseURL + "Puerto Rico/Roosevelt Roads/").map(_.toString)
    val sjPRFiles = getListOfFiles(baseURL + "Puerto Rico/San Juan L M Marin International Airport/").map(_.toString)
    val chSTXFiles = getListOfFiles(baseURL + "St. Croix/Christiansted Hamilton Field Airport/").map(_.toString)
    val caSTTFiles = getListOfFiles(baseURL + "St. Thomas/Charlotte Amalie Cyril E King Airport/").map(_.toString)
    val wdHeader = Source.fromFile(rrPRFiles(0)).getLines.toArray.take(1).mkString

    // val rrPR = rrPRFiles.map(Source.fromFile(_).getLines.toArray.drop(1))
    // val sjPR = sjPRFiles.map(Source.fromFile(_).getLines.toArray.drop(1))
    val chSTX = chSTXFiles.map(Source.fromFile(_).getLines.toArray.drop(1))
    val caSTT = caSTTFiles.map(Source.fromFile(_).getLines.toArray.drop(1))

    val file = new File(baseURL + "weather-data.csv")
    val bw = new BufferedWriter(new FileWriter(file, true))
    bw.write(wdHeader + "\n")
    // for(i <- 0 until rrPR.length) {
    //   for(j <- 0 until rrPR(i).length) {
    //     bw.write(rrPR(i)(j) + "\n")
    //   }
    // }
    // for(i <- 0 until sjPR.length) {
    //   for(j <- 0 until sjPR(i).length) {
    //     bw.write(sjPR(i)(j) + "\n")
    //   }
    // }
    for(i <- 0 until chSTX.length) {
      for(j <- 0 until chSTX(i).length) {
        bw.write(chSTX(i)(j) + "\n")
      }
    }
    for(i <- 0 until caSTT.length) {
      for(j <- 0 until caSTT(i).length) {
        bw.write(caSTT(i)(j) + "\n")
      }
    }
    bw.close()

  }
}
