package br.ufmg.cs.lib.privacy.kanonymity.examples

import br.ufmg.cs.lib.privacy.kanonymity.Mondrian
import br.ufmg.cs.util.Timeable

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TitanicApp extends Timeable {
  def main(args: Array[String]) {
    // args
    val titanicPath = args(0)
    val k = args(1).toInt
    val mode = args(2)
     
    val keyColumns = List("cabin", "age")
    val sensitiveColumns = List("survived")

    val spark = SparkSession.builder().
    master("local[8]").
      config("spark.sql.shuffle.partitions", "8").
      getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val rawData = readTitanic(spark, titanicPath)
    rawData.show

    val mondrian = new Mondrian(rawData, keyColumns, sensitiveColumns, k, mode)
    val mondrianRes = mondrian.result
    val resultDataset = mondrianRes.resultDataset.cache
    println (s"result = ${mondrianRes}")
    println (s"number of anonymized records = ${resultDataset.count}")
    resultDataset.show
    println (s"ncp = ${mondrianRes.ncp}")

    val resultDatasetRev = mondrianRes.resultDatasetRev 
    resultDatasetRev.show

    val anonymizedData = mondrianRes.anonymizedData
    anonymizedData.show
    
    spark.stop()
  }


  // "","pclass","survived","name","sex","age","sibsp","parch","ticket","fare",
  // "cabin","embarked","boat","body","homedest"
  
  case class TitanicRecord(id: String, pclass: String, survived: String,
    name: String, sex: String, age: String, sibsp: String, parch: String,
    ticket: String, fare: String, cabin: String, embarked: String, boat: String,
    body: String, homedest: String)

  def readTitanic(spark: SparkSession, path: String): Dataset[Row] = {
    import spark.implicits._
    val titanic = spark.sparkContext.textFile(path).
      map(_ split ",").flatMap { _fields =>
        val fields = new Array[String](15)
        var i = 0
        var j = 0
        while (i < fields.length) {
          var field = _fields(j)
          j += 1

          if (field.startsWith("\"")) {
            while (!field.endsWith("\"")) {
              field = s"${field},${_fields(j)}"
              j += 1
            }
          }

          fields(i) = field
          i += 1
        }

        try {
          Iterator(TitanicRecord(
            fields(0).replaceAll("\"", "").trim,
            fields(1).replaceAll("\"", "").trim,
            fields(2).replaceAll("\"", "").trim,
            fields(3).replaceAll("\"", "").trim,
            fields(4).replaceAll("\"", "").trim,
            fields(5).replaceAll("\"", "").trim,
            fields(6).replaceAll("\"", "").trim,
            fields(7).replaceAll("\"", "").trim,
            fields(8).replaceAll("\"", "").trim,
            fields(9).replaceAll("\"", "").trim,
            fields(10).replaceAll("\"", "").trim,
            fields(11).replaceAll("\"", "").trim,
            fields(12).replaceAll("\"", "").trim,
            fields(13).replaceAll("\"", "").trim,
            fields(14).replaceAll("\"", "").trim
          ))
        } catch {
          case e: NumberFormatException => Iterator.empty
          case e: Throwable => throw e
        }
      }.mapPartitionsWithIndex { case (idx, iter) =>
        if (idx == 0) {
          iter.drop(1)
        } else {
          iter
        }
      }.toDF()
    titanic
  }
}
