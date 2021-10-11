/*
 * A library of aggregation monoids to be applied on untyped datasets (`DataFrame` or `Dataset[Row]`) as registered
 * udfs.
 *
 * Aggregation functions implement [Aggregator](http://spark.apache.org/docs/latest/sql-ref-functions-udf-aggregate.html)
 * with the caveat that `IN` type is not expected to be a Row.
 *
 * Example usage can be found at test/privacy/spark/AggregationSuite.scala.
 *
 *
 */
package privacy.spark

import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import com.google.privacy.differentialprivacy.{BoundedMean, BoundedQuantiles, BoundedSum, Count}
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.expressions.{Aggregator, Window}

class PrivateCount[T](epsilon: Double, contribution: Int) extends Aggregator[T, Count, Long] {
  override def zero: Count = Count.builder()
    .epsilon(epsilon)
    .maxPartitionsContributed(contribution)
    .build()

  override def reduce(b: Count, a: T): Count = {
    println(a, a.getClass)
    b.increment()
    b
  }

  override def merge(b1: Count, b2: Count): Count = {
    b1.mergeWith(b2.getSerializableSummary)
    b1
  }

  override def finish(reduction: Count): Long = reduction.computeResult().toLong

  override def bufferEncoder: Encoder[Count] = Encoders.kryo[Count]

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

class PrivateSum[T: Numeric](epsilon: Double, contribution: Int, lower: Double, upper: Double) extends Aggregator[T, BoundedSum, Double] {
  override def zero: BoundedSum = BoundedSum.builder()
    .epsilon(epsilon)
    .lower(lower)
    .upper(upper)
    .maxPartitionsContributed(contribution)
    .build()

  override def reduce(b: BoundedSum, a: T): BoundedSum = {
    b.addEntry(implicitly[Numeric[T]].toDouble(a))
    b
  }

  override def merge(b1: BoundedSum, b2: BoundedSum): BoundedSum = {
    b1.mergeWith(b2.getSerializableSummary)
    b1
  }

  override def finish(reduction: BoundedSum): Double = reduction.computeResult

  override def bufferEncoder: Encoder[BoundedSum] = Encoders.kryo[BoundedSum]

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class PrivateMean[T : Numeric](epsilon: Double, contribution: Int, maxContributionsPerPartition: Int, lower: Double, upper: Double) extends Aggregator[T, BoundedMean, Double] {
  override def zero: BoundedMean = BoundedMean
    .builder()
    .epsilon(epsilon)
    .maxPartitionsContributed(contribution)
    .maxContributionsPerPartition(maxContributionsPerPartition)
    .lower(lower)
    .upper(upper)
    .build()

  override def reduce(b: BoundedMean, a: T): BoundedMean = {
    b.addEntry(implicitly[Numeric[T]].toDouble(a))
    b
  }

  override def merge(b1: BoundedMean, b2: BoundedMean): BoundedMean = {
    b1.mergeWith(b2.getSerializableSummary)
    b1
  }

  override def finish(reduction: BoundedMean): Double = reduction.computeResult

  override def bufferEncoder: Encoder[BoundedMean] = Encoders.kryo[BoundedMean]

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

class PrivateQuantiles[T: Numeric](epsilon: Double, contribution: Int, maxContributionsPerPartition: Int, lower: Double, upper: Double, rank: Double = 1.0) extends Aggregator[T, BoundedQuantiles, Double] {
  override def zero: BoundedQuantiles = BoundedQuantiles
    .builder()
    .maxPartitionsContributed(contribution)
    .maxContributionsPerPartition(maxContributionsPerPartition)
    .epsilon(epsilon)
    .lower(lower)
    .upper(upper)
    .build()
  override def reduce(b: BoundedQuantiles, a: T): BoundedQuantiles = {
    b.addEntry(implicitly[Numeric[T]].toDouble(a))
    b
  }

  override def merge(b1: BoundedQuantiles, b2: BoundedQuantiles): BoundedQuantiles = {
    b1.mergeWith(b2.getSerializableSummary)
    b1
  }

  override def finish(reduction: BoundedQuantiles): Double = reduction.computeResult(rank)

  override def bufferEncoder: Encoder[BoundedQuantiles] = Encoders.kryo[BoundedQuantiles]

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object BoundContribution {
  def apply(key: String, contributions: Int)(dataFrame: DataFrame): DataFrame =  {
    val byCol = Window.partitionBy(key)

    val tmpCol = "boundCount"
    dataFrame
      .withColumn(tmpCol, row_number over byCol.orderBy(key))
      .where(col(tmpCol) <= contributions)
      .drop(tmpCol)
  }
}
//
//object countUdf {
//  def apply(epsilon: Double, contribution: Int) = {
//    import org.apache.spark.sql.functions.udaf
//    val cnt = new PrivateCount(epsilon = epsilon,contribution = contribution)
//    val tmp = udaf(cnt)
//    tmp
//  }
//}
