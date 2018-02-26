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

  def getCustomerSales: RDD[(Long, (String, String))] = {
    val yearlyData = saleData.groupBy(sale => (sale._1, sale._4)).map {
      saleSummer => (saleSummer._1._2,saleSummer._1._1,splitToken,splitToken,saleSummer._2.foldLeft(0.toLong)((sum,list)=>
      sum + list._5))
    }

    val monthlyData = saleData.groupBy(sale => (sale._1,sale._2,sale._4)).map {
      saleSummer => (saleSummer._1._3,saleSummer._1._1,saleSummer._1._2,splitToken,
        saleSummer._2.foldLeft(0.toLong)((sum,list)=> sum + list._5))
    }

    val dailyData = saleData.groupBy(sale => (sale._1,sale._2,sale._3,sale._4)).map {
      saleSummer => (saleSummer._1._4,saleSummer._1._1,saleSummer._1._2,saleSummer._1._3,
        saleSummer._2.foldLeft(0.toLong)((sum,list) => sum + list._5))
    }

    val yearlyKeyValue = yearlyData.map( data => (data._1,s"${data._2} ${data._3} ${data._4} ${data._5}"))
    val monthlyKeyValue = monthlyData.map( data => (data._1,s"${data._2} ${data._3} ${data._4} ${data._5}"))
    val dailyKeyValue = dailyData.map( data => (data._1,s"${data._2} ${data._3} ${data._4} ${data._5}"))

    customerData.join(yearlyKeyValue) ++ customerData.join(monthlyKeyValue) ++ customerData.join(dailyKeyValue)
  }

  def result(domain: RDD[(Long,(String,String))]): Unit = {
    val modifiedResult = domain.map (res => res._2._1 + res._2._1).sortBy( x => x)
    modifiedResult.repartition(1).saveAsTextFile("./finalCopy")
  }
}
