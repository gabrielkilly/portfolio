package customml.clustering

class NVect(private val x: Seq[Double]) {
  def +(that: NVect) = new NVect((x, that.x).zipped.map(_+_))
  def -(that: NVect) = new NVect((x, that.x).zipped.map(_-_))
  def *(c: Double) = new NVect(x.map(_*c))
  def /(c: Double) = new NVect(x.map(_/c))
  def dim: Int = x.length

  def distSqr(that: NVect): Double = {
    (x, that.x).zipped.map((a, b) => (a-b)*(a-b)).sum
  }

  def dist(that: NVect): Double = {
    math.sqrt(this.distSqr(that))
  }

  def apply(i: Int): Double = x(i)

  override def equals(that: Any): Boolean = that match {
    case v:NVect => x.length == v.x.length && (x, v.x).zipped.forall((x1, x2) => (x1-x2).abs < 1e-8)
    case _ => false
  }

  override def toString(): String = {
    x.mkString("NVect(", ", ", ")")
  }
}

object NVect {
  def apply(x: Double*) = new NVect(x)

  def average(pnts: Seq[NVect]): NVect = pnts.foldLeft(new NVect(Array.ofDim[Double](pnts.head.x.length)))((acc, x) => acc+x)/pnts.length
}
