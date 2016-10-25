/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803. model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    /** HINT: See Example of Making Patient Vertices Below */
    /**val vertexPatient: RDD[(VertexId, VertexProperty)] = patients
      .map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))**/
    val sc = patients.sparkContext
    // create patient vertex
    //val patientVertexIdRDD = sc.parallelize(Seq(patients.map(_.patientID).distinct()))

    /*val patientVertexIdRDD = diagnostics.
    map(_.patientId).
    distinct.            // get distinct patient ids
    zipWithIndex   */      // assign an index as vertex id


    val patientVertexIdRDD = patients.
      map(_.patientID).
      distinct.map(x => (x.toString,x.toLong))





    /*println("patient vertex")
    patientVertexIdRDD.take(5).foreach(println)
    println(patientVertexIdRDD.count())*/


    val patient2VertexId = patientVertexIdRDD.collect.toMap


    //patient2VertexId.take(5).foreach(println)


    /*val patientVertex = patientVertexIdRDD.
      map{patient => (patient._1, patient.asInstanceOf[VertexProperty])}.
      //map{patient => (patient._1, PatientProperty(patient._1,patient.sex,patient.dob,patient.dod))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]*/


    val patientVertex = patients.
      map{patient => (patient.patientID.toLong,PatientProperty(patient.patientID, patient.sex,patient.dob,patient.dod))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]


    /*patientVertex.take(5).foreach(println)
    println("patient vertices", pa.tientVertex.count())*/


    // create diagnostic code vertex
    val startIndex = patient2VertexId.size+1
    println("startIndex", startIndex)
    val diagnosticVertexIdRDD = diagnostics.
      map(_.icd9code).
      distinct.
      zipWithIndex.
      map{case(icd9code, zeroBasedIndex) =>
        (icd9code, zeroBasedIndex + startIndex)} // make sure no conflict with patient vertex id

    val diagnostic2VertexId = diagnosticVertexIdRDD.collect.toMap

    val diagnosticVertex = diagnosticVertexIdRDD.
      //map{case(icd9code, index) => (index.toString(), DiagnosticProperty(icd9code))}.
      map{case(icd9code, index) => (index, DiagnosticProperty(icd9code))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]

    //println("diagnosis vertices", diagnosticVertex.count())



    //create medication vertex
    val startIndex2 = startIndex+diagnostic2VertexId.size
    val medicationsVertexIdRDD = medications.
      map(_.medicine).
      distinct.
      zipWithIndex.
      map{case(labName, zeroBasedIndex) =>
        (labName, zeroBasedIndex + startIndex2)} // make sure no conflict with other vertex id

    val medications2VertexId = medicationsVertexIdRDD.collect.toMap

    val medicationsVertex = medicationsVertexIdRDD.
      map{case(medicine, index) => (index, MedicationProperty(medicine))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]

    //println("medication vertices", medicationsVertex.count())

    //create Lab result vertex

    val startIndex3 = startIndex2+medications2VertexId.size
    val labResultsVertexIdRDD = labResults.
      map(_.labName).
      distinct.
      zipWithIndex.
      map{case(labName, zeroBasedIndex) =>
        (labName, zeroBasedIndex + startIndex3)} // make sure no conflict with patient vertex id

    val labResults2VertexId = labResultsVertexIdRDD.collect.toMap


    val labResultsVertex = labResultsVertexIdRDD.
      map{case(labName, index) => (index, LabResultProperty(labName))}.
      asInstanceOf[RDD[(VertexId, VertexProperty)]]

    //println("labResults vertices", labResultsVertex.count())


        /** HINT: See Example of Making PatientPatient Edges Below
      *
      * This is just sample edges to give you an example.
      * You can remove this PatientPatient edges and make edges you really need
      * */
    /**case class PatientPatientEdgeProperty(someProperty: SampleEdgeProperty) extends EdgeProperty
    val edgePatientPatient: RDD[Edge[EdgeProperty]] = patients
      .map({p =>
        Edge(p.patientID.toLong, p.patientID.toLong, SampleEdgeProperty("sample").asInstanceOf[EdgeProperty])
      })**/


    val bcDiagnostic2VertexId = sc.broadcast(diagnostic2VertexId)
    val bcPatient2VertexId = sc.broadcast(patient2VertexId)



    /*val patient_diagnostic_edges1: RDD[Edge[EdgeProperty]] = diagnostics.
      map(event => ((event.patientID, event.icd9code,event.sequence), event.date)).
      map{case((patientId, icd9code, sequence), date) => (date.toLong, (patientId, icd9code, sequence))}.reduceByKey( (v1,v2) => { println(v1._1., v2._1); if( v1._1 > v2._1 ) v1 else v2 } ).
      map{case(date, (patientId, icd9code, sequence)) => (patientId, date, icd9code, sequence)}.
      map{case(patientId, date, icd9code, sequence) => Edge(
        bcPatient2VertexId.value(patientId), // src id
        bcDiagnostic2VertexId.value(icd9code), // target id
        PatientDiagnosticEdgeProperty(Diagnostic(patientId,date,icd9code,sequence)) // edge property
      )}*/

    val patient_diagnostic_edges1: RDD[Edge[EdgeProperty]] = diagnostics.
      map(event => ((event.patientID, event.icd9code), (event.date, event.sequence))).reduceByKey( (v1,v2) => {  if( v1._1 > v2._1 ) v1 else v2 } ).
      map{case((patientId,icd9code), (date, sequence)) => (patientId, date, icd9code, sequence)}.
      map{case(patientId, date, icd9code, sequence) => Edge(
        bcPatient2VertexId.value(patientId).toLong, // src id
        bcDiagnostic2VertexId.value(icd9code).toLong, // target id
        PatientDiagnosticEdgeProperty(Diagnostic(patientId,date,icd9code,sequence)) // edge property
      )}



    val patient_diagnostic_edges2: RDD[Edge[EdgeProperty]] = diagnostics.
      map(event => ((event.patientID, event.icd9code), (event.date, event.sequence))).reduceByKey( (v1,v2) => {  if( v1._1 > v2._1 ) v1 else v2 } ).
      map{case((patientId,icd9code), (date, sequence)) => (patientId, date.toLong, icd9code, sequence)}.
      map{case(patientId, date, icd9code, sequence) => Edge(
        bcDiagnostic2VertexId.value(icd9code),// src id
        bcPatient2VertexId.value(patientId),
        PatientDiagnosticEdgeProperty(Diagnostic(patientId,date,icd9code,sequence)) // edge property
      )}


    /*println("patient_diagnostic_edges.count()")
    println( patient_diagnostic_edges1.count()+patient_diagnostic_edges2.count())
    patient_diagnostic_edges1.take(5).foreach(println)
    patient_diagnostic_edges2.take(5).foreach(println)*/

    val bcmedications2VertexId = sc.broadcast(medications2VertexId)

    val patient_medications_edges1: RDD[Edge[EdgeProperty]] = medications.
      map(med => ((med.patientID, med.medicine), (med.date))).reduceByKey( (v1,v2) => {  if( v1 > v2 ) v1 else v2 } ).
      map{case((patientId,medicine), (date )) => (patientId, date, medicine)}.
      map{case(patientId, date, medicine) => Edge(
        bcPatient2VertexId.value(patientId), // src id
        bcmedications2VertexId.value(medicine), // target id
        PatientMedicationEdgeProperty(Medication(patientId,date,medicine)) // edge property
      )}



    val patient_medications_edges2: RDD[Edge[EdgeProperty]] = medications.
      map(med => ((med.patientID, med.medicine), (med.date))).reduceByKey( (v1,v2) => {  if( v1 > v2 ) v1 else v2 } ).
      map{case((patientId,medicine), (date )) => (patientId, date, medicine)}.
      map{case(patientId, date, medicine) => Edge(
        bcmedications2VertexId.value(medicine),// src id
        bcPatient2VertexId.value(patientId),  // target id
        PatientMedicationEdgeProperty(Medication(patientId,date,medicine)) // edge property
      )}

    /*println("patient_medications_edges.count()")
    println( patient_medications_edges1.count()+patient_medications_edges2.count())
    patient_medications_edges1.take(5).foreach(println)
    patient_medications_edges2.take(5).foreach(println)*/

    val bclabResults2VertexId = sc.broadcast(labResults2VertexId)

    val patient_labResults_edges1: RDD[Edge[EdgeProperty]] = labResults.
      map(lab => ((lab.patientID,lab.labName), (lab.date, lab.value))).reduceByKey((v1 ,v2) => {  if( v1._1 > v2._1 ) v1 else v2 } ).
      map{case((patientId,labname),(date, value)) => (patientId, date, labname, value)}.
      map{case(patientId, date, labname, value) => Edge(
        bcPatient2VertexId.value(patientId), // src id
        bclabResults2VertexId.value(labname), // target id
        PatientLabEdgeProperty(LabResult(patientId,date, labname,value))// edge property
      )}

    val patient_labResults_edges2: RDD[Edge[EdgeProperty]] = labResults.
      map(lab => ((lab.patientID,lab.labName), (lab.date, lab.value))).reduceByKey((v1 ,v2) => {  if( v1._1 > v2._1 ) v1 else v2 } ).
      map{case((patientID,labname),(date, value)) => (patientID, date, labname, value)}.
      map{case(patientId, date, labname, value) => Edge(
        bclabResults2VertexId.value(labname),// src id
        bcPatient2VertexId.value(patientId),  // target id
        PatientLabEdgeProperty(LabResult(patientId, date, labname,value))// edge property
      )}



    /*println("patient_labresults_edges.count()")
    println( patient_labResults_edges1.count()+patient_labResults_edges1.count())
    patient_labResults_edges1.take(5).foreach(println)
    patient_labResults_edges2.take(5).foreach(println)*/





    /** example
      * val edges = diagnosticEvents.
    map(event => ((event.patientId, event.eventName), 1)).
    reduceByKey(_ + _).
    map{case((patientId, icd9code), count) => (patientId, icd9code, count)}.
    map{case(patientId, icd9code, count) => Edge(
        bcPatient2VertexId.value(patientId), // src id
        bcDiagnostic2VertexId.value(icd9code), // target id
        count // edge property
    )}

      */


    // Making Graph
    val vertices = sc.union(patientVertex,diagnosticVertex,medicationsVertex,labResultsVertex)

    //println(vertices.count())

    /*val vertices1 = sc.union(patientVertex, diagnosticVertex)
    val graph1 = Graph(vertices1, patient_diagnostic_edges)
    val vertices2 = sc.union(patientVertex, medicationsVertex)
    val graph2 = Graph(vertices2, patient_medications_edges)
    val vertices3 = sc.union(patientVertex, labResultsVertex)
    val graph3 = Graph(vertices3, patient_labResults_edges)*/


    //val edges = patient_diagnostic_edges1.union(patient_medications_edges1).union(patient_labResults_edges1).union(patient_diagnostic_edges2).union(patient_medications_edges2).union(patient_labResults_edges2)
    val edges = sc.union(patient_diagnostic_edges1,patient_diagnostic_edges2,patient_medications_edges1,patient_medications_edges2,patient_labResults_edges1,patient_labResults_edges2)
    //println("after adding edges")

    //val vertices = sc.union(patientVertex, diagnosticVertex)
    val graph:Graph[VertexProperty, EdgeProperty] = Graph(vertices,edges)
    //println("after creating graph")
    //println("edges", edges.count())
    val v = graph.vertices
    //v.take(5).foreach(println)


    //println("before exiting graph loader")
    graph


  }
}
