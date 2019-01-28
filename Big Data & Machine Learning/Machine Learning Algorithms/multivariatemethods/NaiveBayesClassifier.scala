package customml.multivariatemethods
import scala.math._
import customml.BasicStats

class NaiveBayesClassifier(xr: Seq[(Seq[Double], Seq[Double])]) {

  def classify(xs: Seq[Double]): Seq[Double] = {
    val xrDiffs = xr.map{t =>
          var ans = 0.0
          for(pos <- 0 until xs.length)
            ans = ans + pow(xs(pos) - t._1(pos), 2)
          sqrt(ans)
    }
    xr(xrDiffs.indexOf(xrDiffs.min))._2
  }
}

object NaiveBayesClassifier {
  def apply[A](xAndClass: Seq[(Seq[Double], A)]): (NaiveBayesClassifier, Seq[A]) = {
    val rSet = xAndClass.unzip._2.distinct
    var rEncoded = scala.collection.mutable.Seq.fill(rSet.length)(scala.collection.mutable.Seq.fill(rSet.length)(0.0))
    for(i <- 0 until rSet.length)
      rEncoded(i)(i) = 1.0
    val rMap = rSet.zip(rEncoded).toMap
    val classifierSeq = xAndClass.map(tup => (tup._1, rMap(tup._2)))
    (new NaiveBayesClassifier(classifierSeq), rSet)
  }
}
