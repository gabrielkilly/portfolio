package customml.clustering

import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.Buffer

object KMeans {
  /**
   * Takes a sequence of N points and a k for the number of clusters.
   * Returns a sequence of k points for the centers and a sequence that maps the initial N points to the k centers.
   */
  def kMeans(data: Seq[NVect], k: Int): (Seq[NVect], Seq[Int]) = {
    var rands = Random.shuffle((0 until k).toList)
    var cnt = -1
    var centers = mutable.Seq.fill(k){
        cnt += 1
        data(rands(cnt))
    }
    var currCluster = mutable.Seq.fill(data.length)(0)
    for(i <- 0 until 10) {
      var dataByCluster: Seq[Buffer[NVect]] = Seq.fill(k)(Buffer())
      for(vecPos <- 0 until data.length) {
        var distCenters = centers.map(_.dist(data(vecPos)))
        var miniPos = distCenters.indexOf(distCenters.min)
        dataByCluster(miniPos).append(data(vecPos))
        currCluster(vecPos) = miniPos
      }
      for(c <- 0 until centers.length) {
        centers(c) = NVect.average(dataByCluster(c).toSeq)
      }
    }
    (centers, currCluster)
  }
}
