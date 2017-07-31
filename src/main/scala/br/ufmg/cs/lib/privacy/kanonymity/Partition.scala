package br.ufmg.cs.lib.privacy.kanonymity

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * A partition determines the dataset partitioned w.r.t. mondrian anonymity
 * algorithm
 */
case class Partition(
    member: Dataset[Row],
    memberCount: Long,
    low: Array[Int],
    high: Array[Int],
    allow: Array[Int]) {

  override def toString: String = {
    s"Partition(memberlen=${memberCount}" +
    s", low=${low.mkString("[", ", ", "]")}" +
    s", high=${high.mkString("[", ",", "]")}" +
    s", allow=${allow.mkString("[", ",", "]")})"
  }
}

object Partition {
  def apply(member: Dataset[Row], memberCount: Long,
      low: Array[Int], high: Array[Int]): Partition = {
    Partition(member, memberCount, low, high, Array.fill(low.length)(1))
  }
}
