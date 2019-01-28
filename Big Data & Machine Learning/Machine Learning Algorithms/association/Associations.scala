package customml.association

object Associations {
  case class Association[A](given: Seq[A], assume: Seq[A])
  def apriori[A](groupings: Seq[Seq[A]], minSupport: Double, minConfidence: Double): Seq[Association[A]] = {
    ???
  }
}