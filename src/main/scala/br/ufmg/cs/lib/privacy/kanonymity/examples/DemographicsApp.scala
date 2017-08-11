package br.ufmg.cs.lib.privacy.kanonymity.examples

import br.ufmg.cs.lib.privacy.kanonymity.Mondrian
import br.ufmg.cs.util.Timeable

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DemographicsApp extends Timeable {
  def main(args: Array[String]) {
    // args
    val demographicsPath = args(0)
    val conditionsPath = args(1)
    val k = args(2).toInt
    val mode = args(3)

    val spark = SparkSession.builder().
      master("local[4]").
      config("spark.sql.shuffle.partitions", "8").
      getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    val demographics = readDemographics(spark, demographicsPath)
    val conditions = readConditions(spark, conditionsPath)

    val rawData = formatDemographicsData(demographics, conditions).cache

    val keyColumns = List("dobmm", "dobyy", "racex", "educyear", "income")
    val sensitiveColumns = List("icd9codxs")

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

    spark.stop()
  }

  private def formatDemographicsData(demographics: Dataset[Row],
      conditions: Dataset[Row]): Dataset[Row] = {
    // demographics
    val selectedDemographics = demographics.
      select("dupersid", "dobmm", "dobyy", "racex", "educyear", "income").
      groupBy("dupersid").
      agg(first("dobmm").alias("dobmm"), first("dobyy").alias("dobyy"),
        first("racex").alias("racex"), first("educyear").alias("educyear"),
        first("income").alias("income"))

    // conditions
    val selectedConditions = conditions.select("dupersid", "icd9codx")
    val conditionsByDupersid = selectedConditions.
      groupBy("dupersid").
      agg(collect_list("icd9codx").alias("icd9codxs"))

    val joinedDemogConds = selectedDemographics.
      join(conditionsByDupersid, "dupersid").
      select("dobmm", "dobyy", "racex", "educyear", "income", "icd9codxs")

    joinedDemogConds
  }

  // user att
  // ['DUID','PID','DUPERSID','DOBMM','DOBYY','SEX','RACEX','RACEAX','RACEBX',
  // 'RACEWX','RACETHNX','HISPANX','HISPCAT','EDUCYEAR','Year','marry','income',
  // 'poverty']
  case class Demographic(duid: Int, pid: Int, dupersid: String, dobmm: Int,
      dobyy: Int, sex: Int, racex: Int, raceax: Int, racebx: Int, racewx: Int,
      racethnx: Int, hispanx: Int, hipscat: Int, educyear: Int, year: Int,
      marry: Int, income: Int, poverty: Int)
  
  // condition att ['DUID','DUPERSID','ICD9CODX','year']
  case class Condition(duid: Int, dupersid: String,
    icd9codx: String, year: Int)

  def readDemographics(spark: SparkSession, path: String): Dataset[Row] = {
    import spark.implicits._
    val demographics = spark.sparkContext.textFile(path).
      map(_ split ",").flatMap { fields =>
        try {
          Iterator(Demographic(
            fields(0).trim.toInt,
            fields(1).trim.toInt,
            fields(2).replaceAll("\"", ""),
            fields(3).trim.toInt,
            fields(4).trim.toInt,
            fields(5).trim.toInt,
            fields(6).trim.toInt,
            fields(7).trim.toInt,
            fields(8).trim.toInt,
            fields(9).trim.toInt,
            fields(10).trim.toInt,
            fields(11).trim.toInt,
            fields(12).trim.toInt,
            fields(13).trim.toInt,
            fields(14).trim.toInt,
            fields(15).trim.toInt,
            fields(16).trim.toInt,
            fields(17).trim.toInt))
        } catch {
          case e: NumberFormatException => Iterator.empty
          case e: Throwable => throw e
        }
      }.toDF()
    demographics
  }

  def readConditions(spark: SparkSession, path: String): Dataset[Row] = {
    import spark.implicits._
    val conditions = spark.sparkContext.textFile(path).
      map(_ split ",").flatMap { fields =>
        try {
          Iterator(Condition(
            fields(0).replaceAll("\"", "").trim.toInt,
            fields(1).replaceAll("\"", ""),
            fields(2).replaceAll("\"", ""),
            fields(3).trim.toInt))
        } catch {
          case e: NumberFormatException => Iterator.empty
          case e: Throwable => throw e
        }
      }.toDF()
    conditions
  }
}
