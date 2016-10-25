package edu.gatech.cse8803.clustering

/**
  * @author Hang Su <hangsu@gatech.edu>
  */


import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{SparseMatrix, Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._



object NMF {



  /**
   * Run NMF clustering
    *
    * @param V The original non-negative matrix
   * @param k The number of clusters to be formed, also the number of cols in W and number of rows in H
   * @param maxIterations The maximum number of iterations to perform
   * @param convergenceTol The maximum change in error at which convergence occurs.
   * @return two matrixes W and H in RowMatrix and DenseMatrix format respectively 
   */
  def run(V: RowMatrix, k: Int, maxIterations: Int, convergenceTol: Double = 1e-4): (RowMatrix, BDM[Double]) = {

    /**
     * TODO 1: Implement your code here
     * Initialize W, H randomly 
     * Calculate the initial error (Euclidean distance between V and W * H)
     */


    val M = V.numRows().toInt
    println(M)
    val N = V.numCols()
    val sc = V.rows.sparkContext
    //val r = Vector(fromBreeze(Matrix(Math.random())))
    var W = new RowMatrix(V.rows,M, k)
    W.rows.map(l => (math.random))


    //val H = DenseMatrix(k, N, V.rows.toArray())
    var H = BDM.rand[Double](k, V.numCols().toInt)

    println("V", V.numRows(), V.numCols())
    println("W", W.numRows(), W.numCols())
    println("H", H.rows, H.cols)


    //Calculate initial error
    //var err = Math.sqrt(math.pow(V - multiply(W,H),2.0))
    //var err = squaredDistance(V,multiply(W,H))

     /***var rows_1 = V.rows.zip(multiply(W,H).rows).map{case (v1: Vector, v2: Vector) => ((toBreezeVector(v1) :- toBreezeVector(v2)):*(toBreezeVector(v1) :- toBreezeVector(v2))).reduce(_+_)}.map(fromBreeze)

     var err = new RowMatrix(rows_1) ***/

       /**
     * TODO 2: Implement your code here
     * Iteratively update W, H in a parallel fashion until error falls below the tolerance value 
     * The updating equations are, 
     * H = H.* W^T^V ./ (W^T^W H)
     * W = W.* VH^T^ ./ (W H H^T^)
     */

    //H= dotDiv(dotProd(H, computeWTV), H.multiply(W.computeGramianMatrix().multiply(H)))
    //W = dotDiv(dotProd(W, V.multiply(fromBreeze(H).transpose())),H.computeGramianMatrix.multiply(W)

    /*******for (j <- 0 to 10) {

      /* H = H.* W^T^V ./ (W^T^W H)
      * W = W.* VH^T^ ./ (W H H^T^)
      * (V,3688,160)
        (W,3688,3)
        (H,3,160)
      *
      * */

      val WTW = computeWTV(W,W)

      println(WTW.rows, WTW.cols) //(3688,160)

      val WTV = computeWTV(W, V)
      println(WTV.rows, WTV.cols) //(3688,160)

      val H_num = H:*WTV
      println("Hnum", H_num.rows, H_num.cols)
      val H_den =  H:*WTW
      println("Hden", H_den.rows, H_den.cols)

      H = H_num:/H_den
      println("newH", H.rows, H.cols)
     // val W_num = dotProd(W,V).multiply(fromBreeze(H.t))
      val W_num = multiply(dotProd(W,V),H.t)

      println("Wnum", W_num.numRows().toInt, W_num.numCols().toInt)
      val HTH = H*H.t
      val W_den =  multiply(W, HTH)

      println("Wden", W_den.numRows().toInt, W_den.numCols().toInt)
      //W = dotDiv(transpose(W_num), transpose(W_den))
      W = dotProd(transpose(W_num), W_den)
      println("W updated")
       println("W", W.numRows().toInt, W.numCols().toInt)

      val WH = multiply(W,H)

      println(WH.numRows().toInt, WH.numCols().toInt)



      var rows_1 = V.rows.zip(multiply(W,H).rows).map{case (v1: Vector, v2: Vector) => ((toBreezeVector(v1) :- toBreezeVector(v2)):*(toBreezeVector(v1) :- toBreezeVector(v2))).reduce(_+_)}.map(fromBreeze)

      rows_1.take(10)foreach(println)

      var err = math.sqrt(rows_1.flatMap(l => l.toArray).sum)

      if (err <= convergenceTol) {
        break
      }
    }******/


    /** TODO: Remove the placeholder for return and replace with correct values */
    (W, H)
    //(new RowMatrix(V.rows.map(_ => BDV.rand[Double](k)).map(fromBreeze).cache), BDM.rand[Double](k, V.numCols().toInt))
  }


  /**  
  * TODO: Implement the helper functions if you needed
  * Below are recommended helper functions for matrix manipulation
  * For the implementation of the first three helper functions (with a null return), 
  * you can refer to dotProd and dotDiv whose implementation are provided
  */
  /**
  * Note:You can find some helper functions to convert vectors and matrices
  * from breeze library to mllib library and vice versa in package.scala
  */

  /** compute the mutiplication of a RowMatrix and a dense matrix */
  def multiply(X: RowMatrix, d: BDM[Double]): RowMatrix = {

    X.multiply(fromBreeze(d))
  }

  def getContext(X: RowMatrix) : SparkContext = {

    X.rows.sparkContext
  }

  private def getRowMatrix(d : BDM[Double]): RowMatrix = {

    /*var v = Vectors.dense(d.data)

    var k = fromBreeze(d)


    val rows:RDD[Vector] =

    SparkContext sc = SparkC

    new RowMatrix(d.toBreeze(), d.rows,d.cols)

    val rows = toBreezeMatrix(d.t).map{case (v1: Vector) =>
      toBreezeVector(v1)
    }.map(fromBreeze)
    new RowMatrix(rows)*/


    null

  }

 /** get the dense matrix representation for a RowMatrix */
  def getDenseMatrix(X: RowMatrix): BDM[Double] = {
    //new BDM[Double](X.numRows().toInt,X.numCols().toInt,X.rows.collect())
    //new DenseMatrix(X.numRows().toInt,X.numCols().toInt,X.rows.toArray())
    //new BDM[Double](X.numRows().toInt,X.numCols().toInt,X.rows.toArray())



    val values = X.rows.collect().flatMap(l => l.toArray)

    val r = new BDM[Double](X.numRows().toInt, X.numCols().toInt, values)




   //getDenseMatrix(dotProd(V, W))
   r
  }

  ///from Piazza by Brian
  def transpose(W: RowMatrix): RowMatrix = {
    val WTrows = W.rows.zipWithIndex.map{ case(v,i)=>
      val vArray = v.toArray
      val newArray = Array.tabulate(vArray.length){ case(j) => (j,(i,vArray(j))) }
      newArray
    }.flatMap{case(a) => a}.groupByKey.map{case(k,a)=>
      val b = a.map(_._2).toArray
      val v = Vectors.dense(b)
      (k,v)
    }.sortBy(_._1).map(_._2)
    new RowMatrix(WTrows)
  }
  /** matrix multiplication of W.t and V */
  def computeWTV(W: RowMatrix, V: RowMatrix): BDM[Double] = {

     println("inside computeWTV")
    println("W ", W.numRows().toInt, W.numCols().toInt)
    println("W ", V.numRows().toInt, V.numCols().toInt)

     val m = dotProd(V, W)
     val values = m.rows.collect().flatMap(l => l.toArray)
     val r = new BDM[Double]( m.numRows().toInt, m.numCols().toInt, values)

    //getDenseMatrix(dotProd(V, W))
     r


  }

  /** dot product of two RowMatrixes */
  def dotProd(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :* toBreezeVector(v2)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }

  /** dot division of two RowMatrixes */
  def dotDiv(X: RowMatrix, Y: RowMatrix): RowMatrix = {
    val rows = X.rows.zip(Y.rows).map{case (v1: Vector, v2: Vector) =>
      toBreezeVector(v1) :/ toBreezeVector(v2).mapValues(_ + 2.0e-15)
    }.map(fromBreeze)
    new RowMatrix(rows)
  }
}