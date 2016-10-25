/**

  * students: please put your implementation in this file!
  **/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /**
      * Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients.
      * Return a List of patient IDs ordered by the highest to the lowest similarity.
      * For ties, random order is okay
    */

    /** Remove this placeholder and implement your code */

    //get edges for patient id

     //get neighbors for all patient ids
    //println("Inside Jaccard")
    val pvertices = graph.collectNeighborIds(EdgeDirection.Either).filter(n=> n._1 == patientID.asInstanceOf[VertexId]).map(n => (n._2.toSet)).map(n =>n)

    //pvertices.take(10).foreach(println)
    val a = pvertices.first()

    //collection only those neighbors where the id is <999 which will guarantee it is a patientVertex
    val patientneighbors = graph.collectNeighborIds(EdgeDirection.Either).filter(n => n._1.asInstanceOf[VertexId] < 999).map(n => (n._1.toLong, n._2.toSet)).map(n => (jaccard[VertexId](a,n._2),n._1.toLong))
    //println("all other patients")
    //patientneighbors.map(n => if (n._1 == 1.0) n._2).takeOrdered(10).toList


    val similarPatients = patientneighbors.filter(n => n._2 != patientID).sortBy(_._1,false).map(n => n._2).take(10).toList
    //println(similarPatients)

    //List(1,2,3,4,5)
    similarPatients
  }



  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
      * Given a patient, med, diag, lab graph, calculate pairwise similarity between all
      * patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where
      * patient-1-id < patient-2-id to avoid duplications
    */

      /* for each patient collect the patient ids greater than the current id and <999
      and then iterate over current ids set and the collection.
       */
    val sc = graph.edges.sparkContext


    val patientVertices = graph.collectNeighborIds(EdgeDirection.Either).filter(n => n._1.asInstanceOf[VertexId] < 999).map(n => (n._1.toLong, n._2.toSet))

    val res = patientVertices.cartesian(patientVertices)
    val filtered = res.filter(n => n._1._1 < n._2._1)
    val result = filtered.map(n=> (n._1._1,n._2._1,jaccard[VertexId](n._1._2,n._2._2)))

    //println("all other patients")
    //result.take(15).foreach(println)
    result
    //sc.parallelize(Seq((1L, 2L, 0.5d), (1L, 3L, 0.4d)))
  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /**
      * Helper function

      * Given two sets, compute its Jaccard similarity and return its result.
      * If the union part is zero, then return 0.
    */
    
    /** Remove this placeholder and implement your code */
    val u = a.union(b)
    val i = a.intersect(b)
      if (u.size == 0 )
        return 0

    //println("i", i)
    //println("u", u)
    //println(i.size, u.size)

    val res = i.size.toDouble/u.size.toDouble
    //println("jaccard result", res)
    res
  }
}
