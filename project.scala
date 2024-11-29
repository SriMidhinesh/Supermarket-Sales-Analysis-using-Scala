import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object Project {

  // Initialize Spark session
  val spark = SparkSession.builder
    .appName("SupermarketAnalysis")
    .master("local")
    .getOrCreate()
  
  // Read CSV file into a DataFrame, define schema based on file structure
  val filePath = "include your own file path"
  val data = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(filePath)

  data.createOrReplaceTempView("sales") // Register as temporary table

  def topProductsByQuantity(): Unit = {
    val result = spark.sql(
      """SELECT Product_line, SUM(Quantity) AS TotalQuantity
        |FROM sales
        |GROUP BY Product_line
        |ORDER BY TotalQuantity DESC
        |LIMIT 5""".stripMargin)
    result.show()
  }

  def highestRevenueBranch(): Unit = {
    val result = spark.sql(
      """SELECT Branch, SUM(Total) AS TotalRevenue
        |FROM sales
        |GROUP BY Branch
        |ORDER BY TotalRevenue DESC
        |LIMIT 1""".stripMargin)
    result.show()
  }

  def peakSalesTime(): Unit = {
    val result = spark.sql(
      """SELECT HOUR(Time) AS Hour, SUM(Total) AS TotalSales
        |FROM sales
        |GROUP BY HOUR(Time)
        |ORDER BY TotalSales DESC""".stripMargin)
    result.show()
  }

  def salesTrendsOverTime(): Unit = {
    val result = spark.sql(
      """SELECT Date, SUM(Total) AS DailySales
        |FROM sales
        |GROUP BY Date
        |ORDER BY Date""".stripMargin)
    result.show()
  }

  def genderBasedProductPreferences(): Unit = {
    val result = spark.sql(
      """SELECT Gender, Product_line, COUNT(*) AS Count
        |FROM sales
        |GROUP BY Gender, Product_line
        |ORDER BY Gender, Count DESC""".stripMargin)
    result.show()
  }

  def topPaymentMethodsByProductLine(): Unit = {
    val result = spark.sql(
      """SELECT Product_line, Payment, COUNT(*) AS PaymentCount
        |FROM sales
        |GROUP BY Product_line, Payment
        |ORDER BY Product_line, PaymentCount DESC""".stripMargin)
    result.show()
  }

  def profitMarginByProductLine(): Unit = {
    val result = spark.sql(
      """SELECT Product_line, AVG(`gross margin percentage`) AS AvgMargin
        |FROM sales
        |GROUP BY Product_line
        |ORDER BY AvgMargin DESC""".stripMargin)
    result.show()
  }

  def cityWiseRatingAverages(): Unit = {
    val result = spark.sql(
      """SELECT City, AVG(Rating) AS AverageRating
        |FROM sales
        |GROUP BY City
        |ORDER BY AverageRating DESC""".stripMargin)
    result.show()
  }

  def main(args: Array[String]): Unit = {
    // Run the basic and advanced functions
    topProductsByQuantity()
    highestRevenueBranch()
    peakSalesTime()
    salesTrendsOverTime()
    genderBasedProductPreferences()
    topPaymentMethodsByProductLine()
    profitMarginByProductLine()
    cityWiseRatingAverages()

    // Stop the Spark session
    spark.stop()
  }
}
