package customml

import customml._
import scala.util.Random
import scala.math._

class Gaussian(m: Double, v: Double, rand: Random = Random) extends Distribution {
  def next(): Double = (rand.nextGaussian()) * sqrt(v)  + m
  def mean(): Double = m
  def variance(): Double = v

  override def pullN(sz: Int): Seq[Double] = {
    Seq.fill(sz)(next)
  }
}

object Gaussian {
  def apply(data: Seq[Double]): Gaussian = {
    new Gaussian(BasicStats.mean(data), BasicStats.variance(data))
  }
}
