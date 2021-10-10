import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, udaf}
import privacy.spark.{BoundContribution, PrivateCount}

object DayCount extends App {
  final private val DayDataCsv = "day_data.csv"
  final private val DayDataDelimiter = ","

  val spark:SparkSession = SparkSession.builder()
    .appName("DayCount")
    .getOrCreate()
  import spark.implicits._

  val epsilon = Math.log(3)
  val maxContributions = 5

  val dataFrame = spark.read
    .option("delimiter", DayDataDelimiter)
    .option("header", "true")
    .csv(DayDataCsv)

  val privateCount = new PrivateCount(epsilon, maxContributions)
  val privateCountUdf = udaf.register(privateCount)

  dataFrame.transform(BoundContribution("Day", maxContributions))
    .groupBy("Day")
    .agg(privateCountUdf($"VisitorId"))
    .show
}
