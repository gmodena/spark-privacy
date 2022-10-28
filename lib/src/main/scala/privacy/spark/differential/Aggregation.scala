/**
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
package privacy.spark.differential

import org.apache.spark.sql.{DataFrame, Encoder, Encoders, functions}
import com.google.privacy.differentialprivacy.{BoundedMean, BoundedQuantiles, BoundedSum, Count}
import org.apache.spark.sql.functions.{col, rand, row_number, udaf}
import org.apache.spark.sql.expressions.{Aggregator, Window}

class PrivateCount[T](epsilon: Double, contribution: Int) extends Aggregator[T, Count, Long] {
  override def zero: Count = Count.builder()
    .epsilon(epsilon)
    .maxPartitionsContributed(contribution)
    .build()

  override def reduce(b: Count, a: T): Count = {
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

/* A utility function to pre-process `Dataframe`s and bound contribution on input data.
 */
object BoundContribution {
  /**
   * Limit the number of records contributed by  `privacyKey`
   * to at most `maxContributions` in total, and at most `maxContributionsPerPartition`
   * per partition `key`.
   *
   * @param key governs max contributions per partition.
   * @param privacyKey governs max contributions.
   * @param contributions max contributions of privacyKey.
   * @param max contributions of `privacyKey` per partition `key`.
   *
  */
  def apply(key: String, privacyKey: String, maxContributions: Int, maxContributionsPerPartition: Int = 1)(dataFrame: DataFrame): DataFrame =  {
    val byContributionsPerPartition = Window.partitionBy(privacyKey, key)
    val byContributions = Window.partitionBy(privacyKey)
    val contributionsCol = "contributions"
    val contributionsPerPartitionCol = "contributionsPerPartitionCol"

    dataFrame
      .withColumn(contributionsPerPartitionCol, row_number over byContributionsPerPartition.orderBy(rand))
      .where(col(contributionsPerPartitionCol) <= maxContributionsPerPartition)
      .withColumn(contributionsCol, row_number over byContributions.orderBy(rand)) // TODO(gmodena): how secure is this?
      .where(col(contributionsCol) <= maxContributions)
      .drop(contributionsPerPartitionCol, contributionsCol)
  }
}

object aggregations {
  def privateCount[T](epsilon: Double, contribution: Int) = new PrivateCount[T](epsilon, contribution)
  def privateSum[T: Numeric](epsilon: Double, contribution: Int, lower: Double, upper: Double) = new PrivateSum[T](epsilon, contribution, lower, upper)
  def privateMean[T: Numeric](epsilon: Double, contribution: Int, maxContributionsPerPartition: Int, lower: Double, upper: Double) = new PrivateMean[T](epsilon, contribution, maxContributionsPerPartition, lower, upper)
  def privateQuantiles[T: Numeric](epsilon: Double, contribution: Int, maxContributionsPerPartition: Int, lower: Double, upper: Double, rank: Double = 1.0) = new PrivateQuantiles(epsilon, contribution, maxContributionsPerPartition, lower, upper)
}
