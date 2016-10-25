/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
    *
    * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */


      val d1:RDD[FeatureTuple] = diagnostic.map(d=>((d.patientID, d.code), 1.0)).reduceByKey(_+_)

      //println("unfiltered diagnostic" , d1.count())

      d1
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
    *
    * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    //medication.sparkContext.parallelize(List((("patient", "med"), 1.0)))
    val med:RDD[FeatureTuple] = medication.map(m => ((m.patientID, m.medicine), 1.0)).reduceByKey(_+_)

    //println("unfiltered med" , med.count())

    med

  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
    *
    * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    //labResult.sparkContext.parallelize(List((("patient", "lab"), 1.0)))
    //count
      val cnt = labResult.map(l => ((l.patientID, l.testName),1.0)).reduceByKey(_+_)
      val sum = labResult.map(l => ((l.patientID, l.testName),l.value)).reduceByKey(_+_)
      val u = cnt.join(sum)

      val lab:RDD[FeatureTuple] = u.map(l => ((l._1._1,l._1._2), l._2._2/l._2._1))

      //println("unfiltered lab" , lab.count())
      lab
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
    *
    * @param diagnostic RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val diag_candidate:RDD[FeatureTuple] = diagnostic.filter(d => (candiateCode.contains(d.code))).map(d => ((d.patientID, d.code),1.0)).reduceByKey(_+_)
    //diagnostic.sparkContext.parallelize(List((("patient", "diagnostics"), 1.0)))

    println("filtered diagnostic" , diag_candidate.count())
    diag_candidate
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
    *
    * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    //medication.sparkContext.parallelize(List((("patient", "med"), 1.0)))
    val med_candidate:RDD[FeatureTuple] = medication.filter(d => (candidateMedication.contains(d.medicine))).map(d => ((d.patientID, d.medicine),1.0)).reduceByKey(_+_)
    println("filtered med" , med_candidate.count())
    med_candidate
  }


  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
    *
    * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    //labResult.sparkContext.parallelize(List((("patient", "lab"), 1.0)))

    val cnt = labResult.map(l => ((l.patientID, l.testName),1.0)).reduceByKey(_+_)
    val sum = labResult.map(l => ((l.patientID, l.testName),l.value)).reduceByKey(_+_)
    val u = cnt.join(sum).map(l => (l._1._1, l._1._2, l._2._2/l._2._1))

    println("cound and join", u.count())
    u.take(5).foreach(println)

    candidateLab.take(5).foreach(println)

    val lab:RDD[FeatureTuple] = u.filter(l => (candidateLab.contains(l._2.toLowerCase))).map(l =>((l._1,l._2),l._3))


    lab.take(5).foreach(println)

    println("filtered lab" , lab.count())
    lab


  }


  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
    *
    * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map*/
    val feature_names = feature.map(l => l._1._2).distinct()
    val id_map = feature_names.zipWithIndex()

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

     val feature_map = feature.map(l => (l._1._2, (l._1._1, l._2)))

     val temp_group = feature_map.join(id_map).map(l => (l._2._1._1,(l._2._2.toInt, l._2._1._2))).groupByKey()
     //println("temp group")
     //temp_group.take(10).foreach(println)


     val n = feature_names.distinct().count()

     val result = temp_group.map(l => (l._1, Vectors.sparse(n.toInt, l._2.toSeq)))

    /*val result = sc.parallelize(Seq(("Patient-NO-1", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-2", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-3", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-4", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-5", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-6", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-7", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-8", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-9", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
                                    ("Patient-NO-10", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))))*/
    println("result",
      result.count())
    //result.take(10).foreach(println)
    result
    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */
  }
}


