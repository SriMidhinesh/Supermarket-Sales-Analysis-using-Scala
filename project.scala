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

  // 1. Calculate Average Rating using SQL
  def averageRating(): Unit = {
    val result = spark.sql("SELECT AVG(Rating) as AverageRating FROM sales")
    result.show()
  }

  // 2. Calculate Total Sales using SQL
  def totalSales(): Unit = {
    val result = spark.sql("SELECT SUM(Total) as TotalSales FROM sales")
    result.show()
  }

  // 3. Number of Customers by Type using SQL
  def noOfCustomers(): Unit = {
    val result = spark.sql("SELECT Customer_type, COUNT(*) as Count FROM sales GROUP BY Customer_type")
    result.show()
  }

  // 4. Calculate Gender Distribution using SQL
  def calculateGenderDistribution(): Unit = {
    val result = spark.sql("SELECT Gender, COUNT(*) as Count FROM sales GROUP BY Gender")
    result.show()
  }

  // 5. Count Product Types using SQL
  def productTypeCount(): Unit = {
    val result = spark.sql("SELECT Product_line, COUNT(*) as Count FROM sales GROUP BY Product_line")
    result.show()
  }

  // 6. Calculate Sales by City using SQL
  def salesByCity(): Unit = {
    val result = spark.sql("SELECT City, SUM(Total) as TotalSales FROM sales GROUP BY City")
    result.show()
  }

  // 7. Count Payment Methods using SQL
  def paymentMethodCount(): Unit = {
    val result = spark.sql("SELECT Payment, COUNT(*) as Count FROM sales GROUP BY Payment")
    result.show()
  }

  // 8. Calculate Sales by Product Line using SQL
  def salesByProductLine(): Unit = {
    val result = spark.sql("SELECT Product_line, SUM(Total) as TotalSales FROM sales GROUP BY Product_line")
    result.show()
  }

  // 9. Sales by Customer Type using SQL
  def salesByCustomerType(): Unit = {
    val result = spark.sql("SELECT Customer_type, SUM(Total) as TotalSales FROM sales GROUP BY Customer_type")
    result.show()
  }

  // 10. Branch Sales by City using SQL
  def branchSalesByCity(): Unit = {
    val result = spark.sql("SELECT City, Branch, SUM(Total) as TotalSales FROM sales GROUP BY City, Branch")
    result.show()
  }

  def main(args: Array[String]): Unit = {
    // Run the functions
    averageRating()
    totalSales()
    noOfCustomers()
    calculateGenderDistribution()
    productTypeCount()
    salesByCity()
    paymentMethodCount()
    salesByProductLine()
    salesByCustomerType()
    branchSalesByCity()

    // Stop the Spark session
    spark.stop()
  }
}
