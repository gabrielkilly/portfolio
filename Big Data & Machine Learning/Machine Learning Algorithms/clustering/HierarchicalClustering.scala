package customml.clustering

object HierarchicalClustering {
  /**
   * Takes a sequence of N points and a distance function between clusters.
   * Returns a tree with the hierarchical clusters.
   */
  def hClusters(data: Seq[NVect], dist: (Seq[NVect], Seq[NVect]) => Double): HClusterTree = {
    ???
  }

  def findKClusters(tree: HClusterTree, k: Int): Seq[HClusterTree] = {
    ???
  }

  sealed trait HClusterTree extends Seq[NVect] {
    def center = foldLeft(head * 0)((acc, v) => acc + v) / length
    def level: Int
  }

  case class Leaf(vect: NVect) extends HClusterTree {
    def iterator: Iterator[NVect] = new Iterator[NVect] {
      var hasNext: Boolean = true
      def next(): NVect = {
        hasNext = false
        vect
      }
    }
    def apply(idx: Int): NVect = if (idx == 0) vect else throw new IndexOutOfBoundsException("Got index of " + idx + " on a leaf.")
    def length: Int = 1
    def level: Int = 0
  }

  // I wrote this to handle more than two children, but it should probably never have more than two children.
  case class Inner(children: Seq[HClusterTree], level: Int) extends HClusterTree {
    val childrenLengths = children.map(_.length) // This uses a little extra memory, but it improves the efficiency of indexing dramatically
    def iterator = new Iterator[NVect] {
      val childIter = children.iterator
      var curIter = childIter.next.iterator
      def hasNext = curIter.hasNext
      def next = {
        val ret = curIter.next
        if (!curIter.hasNext && childIter.hasNext) curIter = childIter.next.iterator
        ret
      }
    }
    def apply(idx: Int): NVect = {
      val iter = children.zip(childrenLengths).iterator
      var i = idx
      var (c, clen) = iter.next
      while (i >= clen) {
        i -= clen
        val t = iter.next
        c = t._1
        clen = t._2
      }
      c(i)
    }
    val length: Int = childrenLengths.sum
  }
}
