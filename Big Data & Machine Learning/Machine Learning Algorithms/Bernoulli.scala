package customml

import scala.util.Random

/**
 * Bernoulli distribution. Add parameters to the class as needed.
 */
class Bernoulli(prob: Double, rand: Random = Random) extends Distribution {
  def next(): Double = if (rand.nextDouble() < prob) 1 else 0
  def mean(): Double = prob
  def variance(): Double = mean * (1 - mean)

  override def pullN(n: Int): Seq[Double] = {
      Seq.fill(n)(next)
  }
}

object Bernoulli {
  def apply(data: Seq[Double]): Bernoulli = {
          new Bernoulli(data.filter(_ == 1.0).length.toDouble/data.length)
  }
}
