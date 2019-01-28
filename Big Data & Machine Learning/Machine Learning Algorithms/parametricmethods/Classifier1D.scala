package customml.parametricmethods
import scala.math._
import customml.BasicStats
/**
 * This is an implementation of the 1D parametric classifier. The input is a sequence of
 * x and r values. Because it is 1D, here is only one Double for the x and the r is
 * a one-hot encoded sequence.
 */
class Classifier1D(xr: Seq[(Double, Seq[Double])]) {
  // val rSet = xr.unzip._2.distinct
  // val rProbs = rSet.map(r => xr.filter(_._2 == r).length.toDouble / xr.length )
  /**
   * Given a value for x, return one-hot encoding for the class that it belong in.
   */
  def classify(x: Double): Seq[Double] = {
    val diffs = xr.map(v => abs(v._1 - x))
    xr(diffs.indexOf(diffs.min))._2
  }
}

object Classifier1D {
  /**
   * This is an alternate way of making a classifier where the r values are not
   * one-hot encoded. They can be whatever type you want, it should just be a fairly small
   * set of value. The result is both a classifier and a lookup table to let the user
   * know which value of type A corresponds with each class in the one-hot encoded
   * r values that are spit out by the classifier.
   */
  def apply[A](xAndClass: Seq[(Double, A)]): (Classifier1D, Seq[A]) = {
    val rSet = xAndClass.unzip._2.distinct
    var rEncoded = scala.collection.mutable.Seq.fill(rSet.length)(scala.collection.mutable.Seq.fill(rSet.length)(0.0))
    for(i <- 0 until rSet.length)
      rEncoded(i)(i) = 1.0
    val rMap = rSet.zip(rEncoded).toMap
    val classifierSeq = xAndClass.map(tup => (tup._1, rMap(tup._2)))
    (new Classifier1D(classifierSeq), rSet)
  }
}
