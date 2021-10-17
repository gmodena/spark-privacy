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

import org.apache.spark.PrivateCountUDT
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}


class PrivateCount(epsilon: Double, contribution: Int) extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("count", PrivateCountUDT, nullable =  false) :: Nil)

  override def dataType: DataType = ???

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}