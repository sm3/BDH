/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.clustering

import breeze.linalg.{max, sum}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the purity of clustering.
   * Purity is defined as
   *             \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
    *
    * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return purity
   */
  def purity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    /**
     * TODO: Remove the placeholder and implement your code here
     */


    var purity = 0.0
    val N = clusterAssignmentAndLabel.count().toDouble// number of samples
    val K = clusterAssignmentAndLabel.map(_._1).distinct().count().toInt //number of clusters
    val C = clusterAssignmentAndLabel.map(_._2).distinct().count().toInt  //number of classes. should be 3 in our case (1, 2, 3)

    //println("cluster and label")

    //println("count", N)
    //println("num of clusters", K)
    //println("num of classes", C)

    val classes = clusterAssignmentAndLabel.map(_._2).distinct().toArray()
    val clusters = clusterAssignmentAndLabel.map(_._1).distinct().toArray()

    println("classes length", classes.length)

    //var k = K-1.toInt

    var sum = 0.0
    for (i <- 0 to K-1){
      //sum(apply(table(classes, clusters), 2, max)) /
      //val cluster=clusters(i)
      val cluster = i


      var max=0;
      //0, 0), (0, 0), (1, 0), (1, 1), (2, 1), (2, 1), (2, 0)))
      //collect all entries that are in the cluster.
      //(cluster_number, 1), (cluster_number, 2), (cluster_number, 3)
      for (j <- 0 to C-1){
        val class_cl = classes(j)
        //val class_cl = j+1
        val pur  = clusterAssignmentAndLabel.filter(l => ((l._1 == cluster) && (l._2 == class_cl))).count()
        if (max <= pur)
          max = pur.toInt

      }
      sum = sum+ max

    }
    sum/N

  }

  def numbers_report(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {

    val res = 0.0

    val N = clusterAssignmentAndLabel.count().toDouble// number of samples
    val K = clusterAssignmentAndLabel.map(_._1).distinct().count().toInt //number of clusters
    val C = clusterAssignmentAndLabel.map(_._2).distinct().count().toInt  //number of classes. should be 3 in our case (1, 2, 3)

    //println("cluster and label")

    //println("count", N)
    //println("num of clusters", K)
    //println("num of classes", C)

    //val classes = clusterAssignmentAndLabel.map(_._2).distinct().count()
    //val clusters = clusterAssignmentAndLabel.map(_._1).distinct().count()

    val classes = clusterAssignmentAndLabel.map(_._2).distinct().toArray()
    val clusters = clusterAssignmentAndLabel.map(_._1).distinct().toArray()
    println("Classes", classes)
    println("Clusters", clusters)


    //caculate total case, control and others across all 3 clusters

    val cases  = clusterAssignmentAndLabel.filter(l => ((l._2 == 1))).count()
    val control  = clusterAssignmentAndLabel.filter(l => ((l._2 == 2))).count()
    val others  = clusterAssignmentAndLabel.filter(l => ((l._2 == 3))).count()

    println("cases, control, others", cases, control, others)




    for (i <- 0 to K-1){
      //val cluster=clusterAssignmentAndLabel.filter(_._1==i)
      val cluster_case1 = clusterAssignmentAndLabel.filter(l => ((l._1 == i) && (l._2 == 1))).count()
      val cluster_control1 = clusterAssignmentAndLabel.filter(l => ((l._1 == i) && (l._2 == 2))).count()
      val cluster_others1 = clusterAssignmentAndLabel.filter(l => ((l._1 == i) && (l._2 == 3))).count()
      println("cases, control, others in cluster",  i, cluster_case1, cluster_control1, cluster_others1)
      println("Cluster ", i)
      println("percentage Case", (cluster_case1.toDouble/cases.toDouble)*100)
      println("percentage Control", (cluster_control1.toDouble/control.toDouble)*100)
      println("percentage Others", (cluster_others1.toDouble/others.toDouble)*100)
    }

    res

  }

}
