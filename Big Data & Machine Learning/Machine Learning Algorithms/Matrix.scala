package customml

class Matrix(private val values: Seq[Seq[Double]]) {
  def rows: Int = values.length
  def columns: Int = values(0).length
  def apply(row: Int, col: Int): Double = values(row)(col)

  def +(that: Matrix): Matrix = {
    require(rows == that.rows && columns == that.columns)
    var ans = scala.collection.mutable.Seq.fill(rows)(scala.collection.mutable.Seq.fill(columns)(0.0));
    for(r <- 0 until rows)
    {
      for(c <- 0 until columns)
        ans(r)(c) = this(r, c) + that(r, c)
    }
    Matrix(ans)
  }
  def -(that: Matrix): Matrix = {
    require(rows == that.rows && columns == that.columns)
    var ans = scala.collection.mutable.Seq.fill(rows)(scala.collection.mutable.Seq.fill(columns)(0.0));
    for(r <- 0 until rows)
    {
      for(c <- 0 until columns)
        ans(r)(c) = this(r, c) - that(r, c)
    }
    Matrix(ans)
  }
  def *(that: Matrix): Matrix = {
    require(columns == that.rows)
    val tt = that.values.transpose
    val ans = values.map(v => tt.map{ t =>
              var c = -1
              v.map{ no =>
                c+=1
                no * t(c)
              }.sum
            })
   Matrix(ans)
  }
  def *(x: Double): Matrix = {
    var ans = scala.collection.mutable.Seq.fill(rows)(scala.collection.mutable.Seq.fill(columns)(0.0));
    for(r <- 0 until rows)
    {
      for(c <- 0 until columns)
        ans(r)(c) = this(r, c) * x
    }
    Matrix(ans)
  }
  def makeSeq(row: Int, col: Int): Seq[Double] = {
    var ans = scala.collection.mutable.Seq.fill(4)(0.0)
    var curr = 0
    for(r <- 1 until rows) {
      for(c <- 0 until columns) {
        if(c != col)
        {
          ans(curr) = this(r, c)
          curr += 1
        }
      }
    }
    ans
  }
  def det2x2(matrix: Seq[Double]): Double = {
    matrix(0) * matrix(3) - matrix(1) * matrix(2)
  }
  def det: Double = {
    require(rows == columns)
    var currSign = 1
    var ans = 0.0
    if(rows == 2)
      ans = det2x2(values.flatten)
    else {
      for(x <- 0 until columns) {
        ans += currSign * (this(0, x) * det2x2(makeSeq(0, x)))
        currSign *= -1
      }
    }
    ans
  }
  override def toString: String = values.map(_.mkString("|", " ", "|")).mkString("\n")
}

object Matrix {
  def apply(values: Seq[Seq[Double]]): Matrix = new Matrix(values)
}
