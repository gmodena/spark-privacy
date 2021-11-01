[![Project Status: Concept <E2><80><93> Minimal or no implementation has been done yet, or the repository is only intended to be a limited example, demo, or proof-of-concept.](https://www.repostatus.org/badges/latest/concept.svg)](https://www.repostatus.org/#concept)
[![build](https://github.com/gmodena/spark-privacy/actions/workflows/build.yml/badge.svg?branch=devel-wip)](https://github.com/gmodena/spark-privacy/actions/workflows/build.yml)

# spark-privacy

Distributed differential privacy on top of Apache Spark and Google's [differential-privacy](https://github.com/google/differential-privacy/tree/main/examples/java) Java API.

This code is an experimental work-in-progress. API can change and is expected to break compatibility.

## Build

The library can be build with:
```bash
./gradlew build
```

## Private aggregations

The goal of this repo is to explore scalable approaches for DP primitives.

`privacy-spark` wraps the Java API of [differential-privacy](https://github.com/google/differential-privacy/tree/main/examples/java).
Basic Laplacian Mechanism support is currently provided for BoundedMean, BoundedQuantiles, BoundedSum, Count
aggregations.

Upstream code implementation is well suited for divide-and conquer type of computations (they behave like monoids).

`spark-privacy` exposes primitives as Spark UDAFs, using the typed Dataset Aggregator API.

A `PrivateCount` monoid can be implemented with
```scala
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
```
Note that this implementation violates the expected `Aggregator` input for typed Datasets (= a
set of `Row`s). This is currently intended behaviour. UDAFs are expected to be applied to `DataFrame`s by registering them with `functions.udafs`.
This boundary should be made implicit in our implementation.

A `PrivateCount` aggregation can be performed on a group of rows like any built-in aggregate function:

```scala
val privateCount = new PrivateCount[Long](epsilon, maxContributions)                                              
val privateCountUdf = functions.udaf(privateCount)                                                                

dataFrame.groupBy("Day").agg(privateCountUdf($"VisitorId") as "private_count").show 
```

Or with the equivalent SparkSQL API:
```scala
spark.udf.register("private_count", privateCountUdf)
dataFrame.registerTempTable("visitors")
sql.sql("select Day, private_count(*) from visitors group by Day")
```

## Example

An example of private aggregation is available under `examples`.