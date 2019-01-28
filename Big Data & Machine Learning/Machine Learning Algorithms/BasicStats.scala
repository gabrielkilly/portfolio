package customml
import scala.math._

/**
 * Use population formulas instead of sample formulas for all of the places where it applies.
 */
object BasicStats {
  def mean(x: Seq[Double]): Double = {
    x.sum/x.length
  }

  def variance(x: Seq[Double]): Double = {
    x.map(no => pow((no - mean(x)), 2)).sum/x.length
  }

  def stdev(x: Seq[Double]): Double = {
    sqrt(variance(x))
  }

  def covariance(x: Seq[Double], y: Seq[Double]): Double = {
    var count = 0
    var ans = 0.0
    for(count <- 0 until x.length)
      ans += ((x(count) - mean(x)) * (y(count) - mean(y)))
    ans/x.length
  }

  def correlation(x: Seq[Double], y: Seq[Double]): Double = {
    covariance(x,y)/(stdev(x) * stdev(y))
  }

  def weightedMean(x: Seq[Double], weight: Double => Double) = {
    var count = 0
    var total = 0.0
    var sum = 0.0
    for(count <- 0 until x.length)
    {
      total += (x(count) * weight(x(count)))
      sum += weight(x(count))*100
    }
    (total / sum) * 100

  }
}
