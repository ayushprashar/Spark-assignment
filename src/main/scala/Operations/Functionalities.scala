package Operations

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

case class Functionalities(customer: String,sales: String,splitToken: String) {
  val sparkContext: SparkContext = new SparkContext()

  val customerRDD: RDD[Array[String]] = sparkContext.textFile(customer).map( customerRecord => customerRecord.split(splitToken))
  val customerData: RDD[(Long,String)] = customerRDD.map {
    cust => (cust(0).toLong,cust(4))
  }

  val salesRDD: RDD[Array[String]] = sparkContext.textFile(sales).map( saleRecord => saleRecord.split(splitToken))
  val saleData: RDD[(Int, Int, Int, Long, Long)] = salesRDD.map {
    sale => {
      val epoch = new DateTime(sale(0).toLong * 1000L)
      (epoch.getYear,epoch.getMonthOfYear,epoch.getDayOfMonth,sale(1).toLong,sale(2).toLong)
    }
  }

  def getCustomerSales = {
    val yearlyData = (saleData.groupBy(sale => (sale._1,sale._4))).map {
      saleSummer => (saleSummer._)
    }
    val monthlyData = saleData.groupBy(sale => (sale._1,sale._2,sale._4))
    val dailyData = saleData.groupBy(sale => (sale._1,sale._2,sale._3,sale._4))

  }
}
