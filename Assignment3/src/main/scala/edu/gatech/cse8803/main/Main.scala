/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.clustering.{NMF, Metrics}
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import edu.gatech.cse8803.phenotyping.T2dmPhenotype
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors, Vector}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameHolder, SQLContext, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import java.util.Date
import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum}
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._


import scala.io.Source
import java.lang.Double




object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(sqlContext)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    //test_feature_creation(sc)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication)
    )

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    val (kMeansPurity, gaussianMixturePurity, nmfPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] purity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] purity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] purity of NMF is: $nmfPurity%.5f")

    /** feature construction with filtered features */


    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication)
    )

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)

    val (kMeansPurity2, gaussianMixturePurity2, nmfPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] purity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] purity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] purity of NMF is: $nmfPurity2%.5f")


  }



  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures:RDD[(String, Vector)]): (Double, Double, Double) = {
      import org.apache.spark.mllib.linalg.Matrix
      import org.apache.spark.mllib.linalg.distributed.RowMatrix

      /** scale features */
      val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
      val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray)))})
      val rawFeatureVectors = features.map(_._2).cache()

      /** reduce dimension */
      val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
      val pc: Matrix = mat.computePrincipalComponents(2) // Principal components are stored in a local dense matrix.
      val featureVectors = mat.multiply(pc).rows


      val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]
      /** transform a feature into its reduced dimension representation */
      def transform(feature: Vector): Vector = {
        Vectors.dense(Matrices.dense(1, feature.size, feature.toArray).multiply(densePc).toArray)
      }


      /** TODO: K Means Clustering using spark mllib
      *  Train a k means model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 0L
      *  Assign each feature vector to a cluster(predicted Class)
      *  HINT: You might have to use transform function while predicting
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/

      val kmeans_cl = KMeans.train(featureVectors,5,20,1, "k-means||", 0L)


      val predictions = kmeans_cl.predict(featureVectors)

      val pred_features = features.zipWithIndex().map(_.swap).join(predictions.zipWithIndex().map(_.swap)).map(_._2)
      val lbls = pred_features.map(l => (l._1._1, l._2)).join(phenotypeLabel).map(l => l._2)

      val kMeansPurity = Metrics.purity(lbls)

      println("KMeans numbers")
      val num_test1 = Metrics.numbers_report(lbls)
      //val kMeansPurity = 0.0


    /** TODO: GMMM Clustering using spark mllib
      *  Train a Gaussian Mixture model using the variabe featureVectors as input
      *  Set maxIterations =20 and seed as 0L
      *  Assign each feature vector to a cluster(predicted Class)
      *  HINT: You might have to use transform function while predicting
      *  Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
      *  Find Purity using that RDD as an input to Metrics.purity
      *  Remove the placeholder below after your implementation
      **/


    val gmm_cl = new GaussianMixture().setSeed(0L).setK(5).run(featureVectors)

    val g_predictions = gmm_cl.predict(featureVectors)
    val g_pred_features = features.zipWithIndex().map(_.swap).join(g_predictions.zipWithIndex().map(_.swap)).map(_._2)


    val g_lbls = g_pred_features.map(l => (l._1._1, l._2)).join(phenotypeLabel).map(l => l._2)
    //g_lbls.take(25).foreach(println)
    val gaussianMixturePurity = Metrics.purity(g_lbls)
    println("GMM numbers")
    val num_test2 = Metrics.numbers_report(g_lbls)


    //val gaussianMixturePurity = 0.0



    /** NMF */
    /*val rawFeaturesNonnegative = rawFeatures.map({ case (patientID, f)=> Vectors.dense(f.toArray.map(v=>Math.abs(v)))})
    val (w, _) = NMF.run(new RowMatrix(rawFeaturesNonnegative), 3, 100)
    // for each row (patient) in W matrix, the index with the max value should be assigned as its cluster type
    val assignments = w.rows.map(_.toArray.zipWithIndex.maxBy(_._1)._2)

    val labels = features.join(phenotypeLabel).map({ case (patientID, (feature, realClass)) => realClass})

    // zip assignment and label into a tuple for computing purity
    val nmfClusterAssignmentAndLabel = assignments.zipWithIndex().map(_.swap).join(labels.zipWithIndex().map(_.swap)).map(_._2)
    val nmfPurity = Metrics.purity(nmfClusterAssignmentAndLabel)*/
    /** NMF */
    val rawFeaturesNonnegative = rawFeatures.map({ case (patientID, f)=> Vectors.dense(f.toArray.map(v=>Math.abs(v)))})
    val (w, _) = NMF.run(new RowMatrix(rawFeaturesNonnegative), 3, 100)
    // for each row (patient) in W matrix, the index with the max value should be assigned as its cluster type
    val assignments = w.rows.map(_.toArray.zipWithIndex.maxBy(_._1)._2)
    // zip patientIDs with their corresponding cluster assignments
    // Note that map doesn't change the order of rows
    val assignmentsWithPatientIds=features.map({case (patientId,f)=>patientId}).zip(assignments)
    // join your cluster assignments and phenotypeLabel on the patientID and obtain a RDD[(Int,Int)]
    // which is a RDD of (clusterNumber, phenotypeLabel) pairs
    val nmfClusterAssignmentAndLabel = assignmentsWithPatientIds.join(phenotypeLabel).map({case (patientID,value)=>value})
    // Obtain purity value
    val nmfPurity = Metrics.purity(nmfClusterAssignmentAndLabel)



    (kMeansPurity, gaussianMixturePurity, nmfPurity)


  }

  /**
   * load the sets of string for filtering of medication
   * lab result and diagnostics
    *
    * @return
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /** You may need to use this date format. */
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    /** load data using Spark SQL into three RDDs and return them
      * Hint: You can utilize: edu.gatech.cse8803.ioutils.CSVUtils and SQLContext
      *       Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type
      *       Be careful when you deal with String and numbers in String type
      * */

    /** TODO: implement your own code here and remove existing placeholder code below */
    val med  =  CSVUtils.loadCSVAsTable(sqlContext, "data/medication_orders_INPUT.csv", "m")
    val medication: RDD[Medication]  = med.map(m => Medication(m(1).toString,dateFormat.parse(m(11).toString), m(3).toString.toLowerCase))
    //println("medication count", medication.count())
    //medication.take(10).foreach(println)


    val encounter_dx = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_dx_INPUT.csv", "edx")
    val encounter = CSVUtils.loadCSVAsTable(sqlContext, "data/encounter_INPUT.csv", "e")

    var enc = encounter.map(e=> (e(1).toString, (e(2).toString, dateFormat.parse(e(6).toString))))

    //println(enc.count())
    var enc_dx = encounter_dx.map(edx => (edx(5).toString, edx(1).toString))
    //println(enc_dx.count())
    //println(enc.leftOuterJoin(enc_dx).count())
    //enc.leftOuterJoin(enc_dx).take(10).foreach(println)
    val l  =  CSVUtils.loadCSVAsTable(sqlContext, "data/lab_results_INPUT.csv", "lab")
    var labRes = sqlContext.sql("select Member_ID, Date_Resulted, Result_Name, Numeric_Result from lab where Numeric_Result != \"\" ")
    //println(labRes.count())
    //labRes.take(10)


    //val labResult: RDD[LabResult] = labRes.map(k => LabResult(k(1).toString, dateFormat.parse(k(8).toString), k(11).toString, Double.parseDouble(k(14).toString)))
    val labResult: RDD[LabResult] = labRes.map(k => LabResult(k(0).toString, dateFormat.parse(k(1).toString), k(2).toString.toUpperCase, k(3).toString.replaceAll(",", "").toDouble))
    //println("lab count", labResult.count())


    var diagnostic: RDD[Diagnostic] = enc.leftOuterJoin(enc_dx).map(e => Diagnostic(e._2._1._1, e._2._1._2, e._2._2.get.toString))
    //diagnostic.take(10).foreach(println)
    //val diagnostic: RDD[Diagnostic] =  sqlContext.sparkContext.emptyRDD

    /*println("unfiltered count")
    println("medication" , medication.count())
    println("medication distinct" , medication.distinct().count())
    println("labResult" , labResult.count())
    println("labResult distinct" , labResult.distinct().count())
    println("diag" , diagnostic.count())
    println("diag distinct" , diagnostic.distinct().count())*/

    (medication, labResult, diagnostic)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")


  def test_feature_creation(sc: SparkContext) = {
    ////FEATURE CONSTRUCTION TESTS

    //Test Data
    val testMedicationRDD = sc.parallelize(List(
      Medication("pat1", new Date(), "flintstones vitamins"),
      Medication("pat1", new Date(), "flintstones vitamins"),
      Medication("pat1", new Date(), "snake oil"),
      Medication("pat2", new Date(), "snake oil"),
      Medication("pat1", new Date(), "insulin,ultralente"),
      Medication("pat1", new Date(), "insulin,ultralente"),
      Medication("pat1", new Date(), "glucotrol xl"),
      Medication("pat2", new Date(), "glucotrol xl")
    ))

    val testDiagnosticRDD = sc.parallelize(List(
      Diagnostic("pat1", new Date(), "250.00"),
      Diagnostic("pat1", new Date(), "250.00"),
      Diagnostic("pat1", new Date(), "v77.1"),
      Diagnostic("pat2", new Date(), "v77.1"),
      Diagnostic("pat1", new Date(), "911.00"),
      Diagnostic("pat1", new Date(), "911.00"),
      Diagnostic("pat1", new Date(), "sos77.1"),
      Diagnostic("pat2", new Date(), "sos77.1")
    ))

    val testLabratoryRDD = sc.parallelize(List(
      LabResult("pat1", new Date(), "hammer to knee", 10.0),
      LabResult("pat1", new Date(), "hammer to knee", 0.0),
      LabResult("pat1", new Date(), "coughing loudly", 3.0),
      LabResult("pat2", new Date(), "coughing loudly", 4.0),
      LabResult("pat1", new Date(), "hba1c", 8.0),
      LabResult("pat1", new Date(), "hba1c", 1.0),
      LabResult("pat1", new Date(), "glucose, serum", 6.0),
      LabResult("pat2", new Date(), "glucose, serum", 7.0)
    ))

    //Test All Features
    val test_dia_tuples = FeatureConstruction.constructDiagnosticFeatureTuple(testDiagnosticRDD)
    println("Diagnostic Features (All Features)")
    test_dia_tuples.foreach(println)
    println("---")

    val test_med_tuples = FeatureConstruction.constructMedicationFeatureTuple(testMedicationRDD)
    println("Medication Features (All Features)")
    test_med_tuples.foreach(println)
    println("---")

    val test_lab_tuples = FeatureConstruction.constructLabFeatureTuple(testLabratoryRDD)
    println("Lab Features (All Features)")
    test_lab_tuples.foreach(println)
    println("---")

    val test_feature_tuples = sc.union(
      test_dia_tuples,
      test_lab_tuples,
      test_med_tuples
    )
    val raw_test_features = FeatureConstruction.construct(sc, test_feature_tuples)
    println("Feature Vectors (All Features)")
    raw_test_features.foreach(println)
    println("---")
    println(" ")
    println("<<<<<>>>>>")
    println(" ")

    //Test Filtered Features
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    val test_dia_tuples_f = FeatureConstruction.constructDiagnosticFeatureTuple(testDiagnosticRDD, candidateDiagnostic)
    println("Diagnostic Features (Filtered Features)")
    test_dia_tuples_f.foreach(println)
    println("---")

    val test_med_tuples_f = FeatureConstruction.constructMedicationFeatureTuple(testMedicationRDD, candidateMedication)
    println("Medication Features (Filtered Features)")
    test_med_tuples_f.foreach(println)
    println("---")

    val test_lab_tuples_f = FeatureConstruction.constructLabFeatureTuple(testLabratoryRDD, candidateLab)
    println("Lab Features (Filtered Features)")
    test_lab_tuples_f.foreach(println)
    println("---")

    val test_feature_tuples_f = sc.union(
      test_dia_tuples_f,
      test_lab_tuples_f,
      test_med_tuples_f
    )
    val raw_test_features_f = FeatureConstruction.construct(sc, test_feature_tuples_f)
    println("Feature Vectors (Filtered Features)")
    raw_test_features_f.foreach(println)
    println("---")
  }
}
