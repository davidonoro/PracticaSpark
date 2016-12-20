package org.david_onoro

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.dummy.OlympicMedalRecords

object Practica_Ejercicio3 extends App {
  val logFile = "OlympicAthletes.csv"
  val sc = new SparkContext("local", "Simple", "$SPARK_HOME", List("target/spark-practica-1.0.jar"))
  val file = sc.textFile(logFile)

  val olympicMedalRecordsRDD = file.map(x => {
    val arr = x.split(",")
    new OlympicMedalRecords(arr(0), Integer.parseInt(arr(1)), arr(2)
      , Integer.parseInt(arr(3)), arr(5), Integer.parseInt(arr(6)),
      Integer.parseInt(arr(7)), Integer.parseInt(arr(8)))
  }
  )

  olympicMedalRecordsRDD.map(rec=>((rec.getOlympicGame),(rec.getName,rec.getGoldMedals+rec.getSilverMedals+rec.getBronzeMedals)))
    .aggregateByKey(List[(String,Int)]())(
      (acc,curr)=>{
        val lista = curr :: acc
        lista.sortBy(- _._2).take(3)
      },

      (left,right)=>{
        val lista = left ::: right
        lista.sortBy(- _._2).take(3)
      }
    ).sortBy(-_._1)
  .foreach(println)
}


