import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count)
import privacy.spark.{BoundContribution, PrivateCount}

object DayCount extends App {
  final private val DayDataCsv = "day_data.csv"
  final private val DayDataDelimiter = ","

  val spark:SparkSession = SparkSession.builder()
    .appName("DayCount")
    .getOrCreate()

  val epsilon = Math.log(3)
  val maxContributions = 5

  val dataFrame = spark.read
    .option("delimiter", DayDataDelimiter)
    .option("header", "true")
    .csv(DayDataCsv)

  val privateCount = new PrivateCount(epsilon, maxContributions)

  dataFrame.transform(BoundContribution("Day", maxContributions))
    .groupBy("Day").agg(count(col("Day")), privateCount.toColumn.name("private_count"))
    .show
}
