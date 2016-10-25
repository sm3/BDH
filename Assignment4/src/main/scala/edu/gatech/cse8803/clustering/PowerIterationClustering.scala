/**
  * @author Sungtae An <stan84@gatech.edu>.
  */

package edu.gatech.cse8803.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.{PowerIterationClustering => PIC}

/**
  * Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by
  * [[http://www.icml2010.org/papers/387.pdf Lin and Cohen]]. From the abstract: PIC finds a very
  * low-dimensional embedding of a dataset using truncated power iteration on a normalized pair-wise
  * similarity matrix of the data.
  *
  * @see [[http://en.wikipedia.org/wiki/Spectral_clustering Spectral clustering (Wikipedia)]]
  */

object PowerIterationClustering {

  /** run PIC using Spark's PowerIterationClustering implementation
    *
    * @input: All pair similarities in the shape of RDD[(patientID1, patientID2, similarity)]
    * @return: Cluster assignment for each patient in the shape of RDD[(PatientID, Cluster)]
    *
    * */
  def runPIC(similarities: RDD[(Long, Long, Double)]): RDD[(Long, Int)] = {
    val sc = similarities.sparkContext

    /** Remove placeholder code below and run Spark's PIC implementation */

    // Power Iteration Clustering model
     val pic = new PIC().setK(3).setMaxIterations(20)
    val clusteringModel = pic.run(similarities)




    val clustering: RDD[(Long, Int)] = clusteringModel.assignments.map(a => (a.id, a.cluster))
    val cluster1 = clustering.filter(k => k._2 ==1)
    val cluster2 = clustering.filter(k => k._2 ==2)

    cluster1.take(10).foreach(println)
    cluster2.take(10).foreach(println)
    clustering.take(10).foreach(println)
    clustering


  }
}