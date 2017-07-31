package br.ufmg.cs.lib.privacy.kanonymity

import br.ufmg.cs.util.Timeable

import java.util.Arrays

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.immutable.Vector
import scala.collection.mutable.Map
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Hold the result of a mondrian execution.
 * @param mondrian algorithm configuration
 * @param partitions result of a mondrian execution, without evaluation
 */
case class MondrianResult(mondrian: Mondrian,
    partitions: Vector[Partition]) extends Timeable {
  import Mondrian._
  private val spark = mondrian.wholePartition.member.sparkSession

  /**
   * Evaluates this result:
   * - resultDataset: anonymized data
   * - ncp: normalized certainty penalty
   */
  lazy val (resultDataset, ncp): (Dataset[Row], Double) = time {
    import spark.implicits._

    val (qiColumns, qiRange, qiOrder) = (mondrian.qiColumns,
      mondrian.qiRange, mondrian.qiOrder)

    val qiOrderBc = spark.sparkContext.broadcast (qiOrder)

    var (resultDataset, ncp, dp) = partitions.map { partition =>
      val lowBc = spark.sparkContext.broadcast(partition.low)
      val highBc = spark.sparkContext.broadcast(partition.high)

      val _ncp: Double = (0 until mondrian.qiColumns.length).map(
        i => getNormalizedWidth(qiColumns, qiOrder, qiRange, partition, i)
      ).sum * partition.memberCount
      val _dp: Double = scala.math.pow(partition.memberCount, 2)

      val partial = partition.member.rdd.map { r =>
        val originalFields = r.toSeq
        val fields = new Array[Any](originalFields.size)
        var i = 0
        while (i < qiOrderBc.value.length) {
          fields(i) = Row.fromTuple((qiOrderBc.value(i)(lowBc.value(i)),
            qiOrderBc.value(i)(highBc.value(i))))
          i += 1
        }
        while (i < originalFields.length) {
          fields(i) = originalFields(i)
          i += 1
        }
        Row.fromSeq(fields.toSeq)
      }

      (partial, _ncp, _dp)
    }.reduce ( (v1, v2) => (v1, v2) match {
      case ((result1, ncp1, dp1), (result2, ncp2, dp2)) =>
        (result1.union(result2), ncp1 + ncp2, dp1 + dp2)
    })

    ncp = ncp / qiColumns.length
    ncp = ncp / mondrian.wholePartition.memberCount
    ncp = ncp * 100

    val qiSchema = qiColumns.map (
      c => StructField(c, StructType(StructField("low", IntegerType) ::
        StructField("high", IntegerType) :: Nil))
    )

    val schema = StructType (qiSchema ++
      Array(mondrian.wholePartition.member.schema("sensitive_data")))

    val resultDataframe = spark.createDataFrame(resultDataset, schema)

    (resultDataframe, ncp)
  }
}

/**
 * Represent a mondrian setup
 * @param data input data with the following schema
 *  [<qi-att-1>, <qi-att-2>, ..., [sensitive-param-1, sensitive-param-2, ...]]
 * @param k k-anonymity parameter
 * @param mode supported modes: ["strict", "relaxed"]
 */
case class Mondrian(data: Dataset[Row], k: Int,
    mode: String = Mondrian.STRICT) extends Timeable {

  import Mondrian._

  val qiColumns = data.drop("idx", "sensitive_data").columns
  val qiOrder = new Array[Array[Int]](qiColumns.length)
  val qiRange = new Array[Double](qiColumns.length)
  var wholePartition: Partition = _

  lazy val result: MondrianResult = {
    (0 until qiColumns.length).par.foreach { i =>
      qiOrder(i) = data.
        select(qiColumns(i)).
        dropDuplicates().
        sort(qiColumns(i)).
        rdd.
        map(r => r.getInt(0)).
        collect

      qiRange(i) = qiOrder(i)(qiOrder(i).length - 1) - qiOrder(i)(0)

      logInfo(s"qiOrder(${i}): ${qiOrder(i)}")
    }
  
    wholePartition = Partition(data, data.count,
      Array.fill(qiColumns.length)(0), qiOrder.map(order => order.length - 1))
     
    logInfo (s"qiColumns = ${qiColumns.mkString(" ")}")
    logInfo (s"qiOrder = ${qiOrder.mkString(" ")}")
    logInfo (s"qiRange = ${qiRange.mkString(" ")}")
    
    val partitions = mode match {
      case Mondrian.STRICT =>
        anonymizeStrict(qiColumns, qiOrder, qiRange, wholePartition, k)
      case Mondrian.RELAXED =>
        anonymizeRelaxed(qiColumns, qiOrder, qiRange, wholePartition, k)
      case _ =>
        throw new RuntimeException(s"Unknown mondrian mode: ${mode}")
    }

    MondrianResult(this, partitions)
  }

  private def anonymizeStrict(qiColumns: Array[String],
      qiOrder: Array[Array[Int]], qiRange: Array[Double],
      partition: Partition,
      k: Int): Vector[Partition] = time {
    println (s"anonymizeStrict ${partition}")
    val allowCount = partition.allow.sum
    if (allowCount == 0) {
      return Vector(partition)
    }

    var i = 0
    while (i < allowCount) {
      val dim = chooseDimension(qiColumns, qiOrder, qiRange, partition)
      assert (dim != -1,
        s"${qiColumns.toSeq} ${qiOrder.toSeq} ${qiRange} ${partition}")

      val (splitValOpt, nextValOpt, lowOpt, highOpt) = findMedian(
        qiColumns, partition, dim, k)
        
      val order = qiOrder(dim)

      if (lowOpt.isDefined) {
        val firstIdx = Arrays.binarySearch(order, lowOpt.get)
        val secondIdx = Arrays.binarySearch(order, highOpt.get)
        val firstAtt = order(firstIdx)
        val secondAtt = order(secondIdx)

        if (firstAtt == lowOpt.get) {
          partition.low(dim) = firstIdx
          partition.high(dim) = secondIdx
        } else if (firstAtt == highOpt.get) {
          partition.low(dim) = secondIdx
          partition.high(dim) = firstIdx
        } else {
          throw new RuntimeException(s"Should never happen:")
        }
      }

      if (!splitValOpt.isDefined || splitValOpt == nextValOpt) {
        partition.allow(dim) = 0
      } else {
        val mean = Arrays.binarySearch(order, splitValOpt.get)
        val nextValIdx = Arrays.binarySearch(order, nextValOpt.get)

        val lhsHigh = new Array[Int](partition.high.length)
        Array.copy(partition.high, 0, lhsHigh, 0, lhsHigh.length)
        lhsHigh(dim) = mean

        val rhsLow = new Array[Int](partition.low.length)
        Array.copy(partition.low, 0, rhsLow, 0, rhsLow.length)
        rhsLow(dim) = nextValIdx

        import partition.member.sparkSession.implicits._

        val lhsIdxs = partition.member.sparkSession.sparkContext.
          parallelize (order.slice(0, mean + 1)).
          toDF(s"${qiColumns(dim)}")

        val rhsIdxs = partition.member.sparkSession.sparkContext.
          parallelize (order.slice(mean + 1, order.length)).
          toDF(s"${qiColumns(dim)}")

        val Array((lhsMember, lhsMemberCount), (rhsMember, rhsMemberCount)) =
          Array(lhsIdxs, rhsIdxs).par.map { idxs =>
            val member = partition.member.
              join(idxs, s"${qiColumns(dim)}").cache
            (member, member.count)
          }.toArray
        
        if (lhsMemberCount < k || rhsMemberCount < k) {
          partition.allow(dim) = 0
        } else {
          val lhs = Partition(lhsMember, lhsMemberCount, partition.low, lhsHigh)
          val rhs = Partition(rhsMember, rhsMemberCount, rhsLow, partition.high)

          val Array(lhsRes, rhsRes) = Array(lhs, rhs).par.map { hs =>
            anonymizeStrict(qiColumns, qiOrder, qiRange, hs, k)
          }.toArray
  
          return lhsRes ++ rhsRes
        }
      }
      i += 1
    }
    Vector(partition)
  }

  private def anonymizeRelaxed(qiColumns: Array[String],
      qiOrder: Array[Array[Int]], qiRange: Array[Double],
      partition: Partition,
      k: Int): Vector[Partition] = ???
}

object Mondrian {
  
  val STRICT = "strict"
  val RELAXED = "relaxed"

  def findMedian(qiColumns: Array[String],
      partition: Partition,
      dim: Int,
      k: Int): (Option[Int], Option[Int],
                Option[Int], Option[Int]) = {

    var splitValOpt: Option[Int] = None
    var nextValOpt: Option[Int] = None
    val frequency = frequencySet(qiColumns, partition, dim)
    val frequencyLocal = frequency.rdd.map {
      case Row(att: Int, frequency: Long) => (att, frequency)
    }.collect
    val total = frequencyLocal.map(_._2).sum
    val middle = total / 2

    val (lowOpt, highOpt) = if (frequencyLocal.length == 0) {
      (None, None)
    } else {
      (Some(frequencyLocal(0)._1),
        Some(frequencyLocal(frequencyLocal.length - 1)._1))
    }

    if (middle < k || !lowOpt.isDefined || !highOpt.isDefined) {
      return (splitValOpt, nextValOpt, lowOpt, highOpt)
    }

    var i = 0
    var accum: Long = 0
    var break = false
    while (i < frequencyLocal.length && !break) {
      val (att, frequency) = frequencyLocal(i)
      accum += frequency
      if (accum >= middle) {
        splitValOpt = Some(att)
        break = true
      }
      i += 1
    }

    if (i < frequencyLocal.length) {
      val (att, frequency) = frequencyLocal(i)
      nextValOpt = Some(att)
    } else {
      nextValOpt = splitValOpt
    }

    (splitValOpt, nextValOpt, lowOpt, highOpt)
  }

  def frequencySet(qiColumns: Array[String],
      partition: Partition,
      dim: Int): Dataset[Row] = {
    val frequency = partition.member.
      select(qiColumns(dim)).
      groupBy(qiColumns(dim)).
      count().
      sort(qiColumns(dim)).
      withColumnRenamed("count", "frequency")
    frequency
  }

  def getNormalizedWidth(
      qiColumns: Array[String],
      qiOrder: Array[Array[Int]],
      qiRange: Array[Double],
      partition: Partition,
      index: Int): Double = {
    val order = qiOrder(index)
    val highIndex = partition.high(index)
    val lowIndex = partition.low(index)
    val width = order(highIndex) - order(lowIndex)

    width * 1.0 / qiRange(index)
  }

  def chooseDimension(
      qiColumns: Array[String],
      qiOrder: Array[Array[Int]],
      qiRange: Array[Double],
      partition: Partition): Int = {
    var maxWidth = Double.MinValue
    var maxDim = Int.MinValue
    var dim = 0
    while (dim < qiColumns.length) {
      if (partition.allow(dim) != 0) {
        val normWidth = getNormalizedWidth(qiColumns,
          qiOrder, qiRange, partition, dim)
        if (normWidth > maxWidth) {
          maxWidth = normWidth
          maxDim = dim
        }
      }
      dim += 1
    }
    maxDim
  }
}
