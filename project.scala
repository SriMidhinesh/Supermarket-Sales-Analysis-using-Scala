import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map

object project {

  val sc = new SparkContext("local", "term")
  val lines = sc.textFile(path = "D:\\Users\\pobba\\Downloads\\supermarket_sales - Sheet1.csv")

  val data = lines.filter(line => !line.startsWith("Invoice ID"))
  //data.foreach(println)

  def averageRating(data_1: RDD[String]): Double = {

    val a = data_1.map(line => line.split(",")(16).toDouble)
    val b = a.reduce((a, b) => a + b)
    val ratingCount = a.count()
    val averageRating = b / ratingCount

    averageRating

  }

  def totalSales(data_2:RDD[String]):Double = {

    val totalSales = data_2.map(line => line.split(",")(9).toDouble)
    val total = totalSales.sum()

    total

  }

  def noOfCustomers(data_3: RDD[String]): Unit = {

    val customerCounts = data_3.map(line => line.split(",")(3))
      .countByValue()
    customerCounts.foreach { case (customer_type, count) =>
      println(s"Gender: $customer_type, Count: $count")
    }

  }

  def calculateGenderDistribution(data_4: RDD[String]): Unit = {
    val genderDistribution = data_4.map(line => line.split(",")(4)).countByValue()
    genderDistribution.foreach { case (gender, count) =>
      println(s"Gender: $gender, Count: $count")
    }
  }

  def productTypeCount(data_5: RDD[String]): Unit = {

        val productLines = data_5.map(line => line.split(",")(5)).countByValue()
        print(productLines)
        productLines.foreach { case (producttype, count) =>
          println(s"producttype:$producttype,Count: $count")
        }

  }

  def salesByCity(data_6:RDD[String]):Unit = {

    val salesByCity = data_6.map(_.split(","))
          .groupBy(_(2))
          .map { case (city, sales) =>
            val totalSales = sales.map(_(9).toDouble).sum
            (city, totalSales)
          }

        salesByCity.foreach { case (city, totalSales) =>
          println(s"City: $city, Total Sales: $totalSales")
        }

  }

  def paymentMethodCount(data_7:RDD[String]): Unit = {

    val paymentMethodCounts = data_7.map(line => line.split(",")(12))
          .countByValue()
        paymentMethodCounts.foreach { case (paymentMethod, count) => println(s"Payment ID: $paymentMethod - Count: $count")
        }

  }

  def salesByProductLine(data_8:RDD[String]): Unit = {

    val productSales = data_8.map(line => {
            val values = line.split(",")
            val productLine = values(5)
            val totalSales = values(9).toDouble
            (productLine, totalSales)
          })

          val salesByProductLine = productSales.groupBy(_._1) // Group by product line

          salesByProductLine.foreach { case (productLine, sales) =>
            val totalSales = sales.map(_._2).sum
            println(s"Product Line: $productLine, Total Sales: $totalSales")
          }

  }

  def salesByCustomerType(data_9:RDD[String]): Unit = {

    val SalesbyCustomertype = data.map(line => {
            val values = line.split(",")
            val customertype = values(3)
            val Sales = values(9).toDouble
            (customertype, Sales)
          })

          val total_sales = SalesbyCustomertype.groupBy(_._1)

          total_sales.foreach { case (customer, totalsales) =>
            val totalSales = totalsales.map(_._2).sum
            println(s"Customer Type: $customer - Total Sales: $totalSales")
          }

  }

  def branchSalesByCity(data_10:RDD[String]):Unit = {

        val branchSales = data_10.map(line => {
          val values = line.split(",")
          val branch = values(1)
          val city = values(2)
          val totalSales = values(9).toDouble
          (branch, city, totalSales)
        })

        val branchSalesByCity = branchSales.groupBy(_._2) // Group by city

        branchSalesByCity.foreach { case (city, citySales) =>
          println(s"City: $city")
          val branchSales = citySales.groupBy(_._1) // Group by branch within the city
          branchSales.foreach { case (branch, sales) =>
            val totalSales = sales.map(_._3).sum
            println(s"Branch: $branch - Total Sales: $totalSales")
          }
        }

  }



  def main(args: Array[String]): Unit = {

    println(averageRating(data))
    println(totalSales(data))
    noOfCustomers(data)
    calculateGenderDistribution(data)
    productTypeCount(data)
    salesByCity(data)
    paymentMethodCount(data)
    salesByProductLine(data)
    salesByCustomerType(data)
    branchSalesByCity(data)


    val paymentMethod=data.map(line => line.split(",")(12).toDouble).collect()

    val H=breeze.plot.hist(DenseVector(paymentMethod))
    val f=breeze.plot.Figure()
    val plt=f.subplot(0)
    plt+=H
    plt.xlabel = "Payment Method"
    plt.ylabel = "Frequency"
    plt.title = "Payment Method Distribution"
    f.refresh()

  }
}