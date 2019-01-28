package customml
import scala.util.Random

class Binomial(n: Double, prob: Double, rand: Random = Random) extends Distribution {
  def next(): Double = if (rand.nextDouble() < prob) 1 else 0
  def mean(): Double = prob * n
  def variance(): Double = mean * (1 - prob)

  override def pullN(sz: Int): Seq[Double] = {
    Seq.fill(sz)((Seq.fill(n.toInt)(next)).sum)
  }
}

object Binomial {
  def apply(n: Int, data: Seq[Double]): Binomial = {
    new Binomial(n, data.map(no => no/n).sum/data.length)
  }
}
