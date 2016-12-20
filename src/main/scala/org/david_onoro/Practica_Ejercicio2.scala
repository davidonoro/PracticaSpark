package org.david_onoro

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.dummy.OlympicMedalRecords

object Practica_Ejercicio2 extends App {
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

  olympicMedalRecordsRDD.map(rec=>((rec.getCountry,rec.getOlympicGame),rec.getGoldMedals*3+rec.getSilverMedals*2+rec.getBronzeMedals))
  .reduceByKey(_+_)
    .map(x=>(x._1._1,(x._1._2,x._2)))
    .reduceByKey((acc,curr)=>{
        if (curr._2 > acc._2){
          (curr._1,curr._2)
        }else{
          (acc._1,acc._2)
        }
  })
    .sortBy(- _._2._2)
  .foreach(x=> println(x._1+"\t"+x._2._1+"\t"+x._2._2))
}


