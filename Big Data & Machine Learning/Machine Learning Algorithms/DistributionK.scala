package customml

trait DistributionK {
  def next(): Seq[Double]
  def mean(): Seq[Double]
  def variance(): Seq[Double]

  def pullN(n: Int): Seq[Seq[Double]] = ???
}
