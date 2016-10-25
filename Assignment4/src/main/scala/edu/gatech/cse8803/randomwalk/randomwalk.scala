package edu.gatech.cse8803.randomwalk

import edu.gatech.cse8803.model.{PatientProperty, EdgeProperty, VertexProperty}
import org.apache.spark.graphx._

object RandomWalk {

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, numIter: Int = 100, alpha: Double = 0.15): List[Long] = {
    /**
      * Given a patient ID, compute the random walk probability w.r.t. to all other patients.
      * Return a List of patient IDs ordered by the highest to the lowest similarity.
      * For ties, random order is okay
      */

    /** Remove this placeholder and implement your code -added code from spark source code*/



    //println("inside randomwalk")



    val personalized = true
    val src: VertexId = patientID.asInstanceOf[VertexId]

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute resetProb.
    // When running personalized pagerank, only the source vertex
    // has an attribute resetProb. All others are set to 0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) =>
      if (!(id != src && personalized)) alpha else 0.0
    }


    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }


    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()


      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)


      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      val rPrb = if (personalized) {
        (src: VertexId, id: VertexId) => alpha * delta(src, id)
      } else {
        (src: VertexId, id: VertexId) => alpha
      }


      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - alpha) * msgSum
      }.cache()


      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices

      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)


      iteration += 1

    }


    //println("ranks")
    //rankGraph.vertices.take(10).foreach(println)


    val res = rankGraph.vertices.map(k =>  (k._1,k._2)).filter(i => i._1 <999).sortBy(k => k._2,ascending = false).filter(k => k._1 != src).map(k => k._1).take(10).toList


    //val result = res.take(15).toList
    //println("res")
    //print(res)

    //// get top 5 most similar
    //sssp.vertices.filter(_._2 < Double.PositiveInfinity).filter(_._1 < 300).takeOrdered(5)(scala.Ordering.by(-_._2))

    //List(1,2,3,4,5)
    res

  }

}

