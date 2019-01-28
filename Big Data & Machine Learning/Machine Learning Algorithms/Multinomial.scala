package customml

import scala.util.Random

class Multinomial(n: Int, probs: Seq[Double], rand: Random = Random) extends DistributionK {
    def indexFinder(): Int = {
        val r = rand.nextDouble
        var tot = 0.0
        probs.zipWithIndex.foldRight(-1){ (tup, acc) =>
          tot += tup._1
          if(tot > r)
            tup._2
          acc
        }
    }

    def next(): Seq[Double] = {
        var arr = Seq.fill(probs.length)(0.0).toArray
        var cnt = n
        var index = 0
        while (cnt > 0) {
            index = indexFinder
            arr(index) += 1.0
            cnt -= 1
        }
        arr
    }

    def mean(): Seq[Double] = probs.map(no => n * no)
    def variance(): Seq[Double] = probs.map(no => n * no * (1 - no))

    override def pullN(sz: Int): Seq[Seq[Double]] = {
      Seq.fill(sz)(next)
    }
}

object Multinomial {
    def apply(n: Int, data: Seq[Seq[Double]]): Multinomial = {
        var cnt = -1
        val probs = Seq.fill(data.head.length){
          cnt += 1
          data.map(s => (s(cnt))).sum / (data.length.toDouble * n)
        }
        new Multinomial(n, probs)
    }
}
