/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import breeze.linalg.max
import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD

object T2dmPhenotype {
  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    *
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Remove the place holder and implement your code here.
      * Hard code the medication, lab, icd code etc for phenotype like example code below
      * as while testing your code we expect your function have no side effect.
      * i.e. Do NOT read from file or write file
      *
      * You don't need to follow the example placeholder codes below exactly, once you return the same type of return.
      */

    val sc = medication.sparkContext

    /** Hard code the criteria */
    val type1_dm_dx = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43", "250.51", "250.53", "250.61"
      ,"250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")
    val type1_dm_med = Set("lantus", "insulin glargine" ,"insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

    val type2_dm_dx = Set("250.3","250.32","250.2","250.22","250.9","250.92","250.8","250.82","250.7","250.72","250.6","250.62","250.5","250.52","250.4","250.42","250.00","250.02")

    val type2_dm_med = Set("chlorpropamide","diabinese","diabanase","diabinase","glipizide","glucotrol","glucotrol XL","glucatrol","glyburide","micronase","glynase","diabetamide",
                       "diabeta","glimepiride","amaryl","repaglinide","prandin","nateglinide","metformin","rosiglitazone","pioglitazone", "acarbose","miglitol","sitagliptin",
                       "exenatide","tolazamide","acetohexamide","troglitazone","tolbutamide","avandia","actos","ACTOS","glipizide")


    val abnormal_lab_values_dx = Set(("HbA1c",6.0),("Hemoglobin A1c" , 6.0)  ,("Fasting Glucose", 110), ("Fasting blood glucose", 110), ("fasting plasma glucose",110 ), ("Glucose", 110), ("glucose", 110),("Glucose, Serum", 110))

    val dm_related_dx = Set("790.21","790.22","790.2","790.29","648.81","648.82","648.83","648.84","648","648","648.01","648.02","648.03","648.04","791.5","277.7","V77.1","256.4","250.*")


    /** find case patients */

    //total patients
    val total_patients:RDD[String] = diagnostic.map(_.patientID).union(medication.map(_.patientID)).union(labResult.map(_.patientID)).distinct()


    //type1_dm = YES
    val cp1 = diagnostic.filter(d => type1_dm_dx.contains(d.code)).map(_.patientID).distinct()

    val type1_dm_no = total_patients.subtract(cp1)
    //println("patients with type1 dm = NO", type1_dm_no.distinct().count())

    //type1_dm = NO && type2_dm = YES && type1_med = NO

    val cp1_1= diagnostic.filter(d => type2_dm_dx.contains(d.code)).map(_.patientID).distinct()

    val temp = type1_dm_no.intersection(cp1_1)

    //println("patients with type2 dm = Yes", temp.count())
    //println("Medication count", medication.count())

    val type1_med_yes = medication.filter(d => type1_dm_med.contains(d.medicine.toString)).map(_.patientID).distinct()
    type1_med_yes.take(5).foreach(println)

    //println("type1_med_yes  count  ",type1_med_yes.count())
    val type1_med_no = temp.subtract(type1_med_yes)
    //println("patients with type1 Med = No", type1_med_no.distinct().count())

    val firstpath = temp.intersection(type1_med_no)
    //println("first path case patients", firstpath.distinct().count())
    //val cp2_1 = diagnostic.filter{d => !type1_dm_dx.contains(d.code) && type2_dm_dx.contains(d.code)}.map(_.patientID).distinct()
    //val cp2_2 = medication.filter(d => !type1_dm_med.contains(d.medicine.toLowerCase)).map(_.patientID).distinct()

    val cp2 = temp.intersection(type1_med_yes)
    //println("type1_med = yes", cp2.count())

    //type1_dm = NO && type2_dm = YES && type1_med = yes && type2_med = NO

    val type2_med_yes = medication.filter(d => type2_dm_med.contains(d.medicine.toLowerCase) ).map(_.patientID).distinct()
    val type2_med_no = total_patients.subtract(type2_med_yes)
    val secondpath = cp2.intersection(type2_med_no)

    //println("second path", secondpath.count())

    //type1_dm = NO && type2_dm = YES && type1_med = yes && type2_med = YES  && type2_med(date) < type1_med(date)

    val cp3 = cp2.intersection(type2_med_yes)

    //println("type 1 and type med yes", cp3.count())
    val dt1 = medication.filter(d => type1_dm_med.contains(d.medicine.toLowerCase)).map(d => (d.patientID, d.date.getTime())).reduceByKey(Math.min)
    val dt2 = medication.filter(d => type2_dm_med.contains(d.medicine.toLowerCase)).map(d => (d.patientID, d.date.getTime())).reduceByKey(Math.min)

    val cp4 = dt1.join(dt2).filter(d => d._2._1 > d._2._2).map(_._1)

    val thirdpath = cp3.intersection((cp4))

    //println("third path", thirdpath.count())


    val casePatients = sc.union(firstpath, secondpath, thirdpath).map((_, 1)).distinct()



    /** Find CONTROL Patients */
    //glucose tests have "glucose" or HbA1c in the name. Filter out records containing either of them and take the union - use "glucose" only
    val ct1 = labResult.filter(d=> d.testName.contains("glucose".toUpperCase)).map(d => LabResult(d.patientID, d.date, d.testName, d.value)).distinct()

    val temp2 = ct1.map(_.patientID).distinct()
    val glucose = total_patients.intersection(temp2)

    //println("glucose measure = yes" , glucose.count())
    //from all records with some type of glucose test check for abnormal results
    val c1 = labResult.filter(d => d.testName == "HbA1c".toUpperCase && d.value >= 6.0).map(_.patientID).distinct()
    val c2 = labResult.filter(d => d.testName == "Hemoglobin A1c".toUpperCase && d.value >= 6.0).map(_.patientID).distinct()
    val c3 = labResult.filter(d => d.testName == "Fasting Glucose".toUpperCase && d.value >= 110).map(_.patientID).distinct()
    val c4 = labResult.filter(d => d.testName == "Fasting blood glucose".toUpperCase && d.value >= 110).map(_.patientID).distinct()
    val c5 = labResult.filter(d => d.testName == "fasting plasma glucose".toUpperCase && d.value >= 110).map(_.patientID).distinct()
    val c6 = labResult.filter(d => d.testName == "Glucose".toUpperCase && d.value > 110).map(_.patientID).distinct()
    val c7 = labResult.filter(d => d.testName == "Glucose, Serum".toUpperCase && d.value > 110).map(_.patientID).distinct()
    val c8 = labResult.filter(d => d.testName == "glucose".toUpperCase && d.value > 110).map(_.patientID).distinct()


    val abnormal = sc.union(c1, c2, c3, c4, c5, c6, c7, c8).distinct()

    val normal_glucose = glucose.subtract(abnormal)

    //println("abnormal NO and glucose test = yes", normal_glucose.count() )

    //filter DM related from diag
    val dm_diag1 = diagnostic.filter(d => dm_related_dx.contains(d.code)).map(_.patientID).distinct()
    val dm_diag2 = diagnostic.filter(d => d.code.startsWith("250.")).map(_.patientID).distinct()

    val dm_diag = dm_diag1.union(dm_diag2)

    //println("dm yes", dm_diag.count())

    val controlPatients = normal_glucose.subtract(dm_diag).map((_,2)).distinct()
    //println("control", controlPatients.count())
    //controlPatients.take(10).foreach(println)

    /** Find OTHER Patients */

    val not_others:RDD[String] = casePatients.union(controlPatients).map(_._1)
    val others = total_patients.subtract(not_others).map((_,3)).distinct()

    /*println("*****count*****")
    println(total_patients.count())
    println(casePatients.count())
    println(controlPatients.count())
    println(others.count())*/

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)
    //val phenotypeLabel: Null = null
    /** Return */
    phenotypeLabel
  }
}
