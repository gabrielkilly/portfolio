package customml

/**
 * Supertype for the distributions.
 */
trait Distribution {
  def next(): Double
  def mean(): Double
  def variance(): Double

  def pullN(n: Int): Seq[Double] = ???
}
