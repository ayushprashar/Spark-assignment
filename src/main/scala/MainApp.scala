import Operations.Functionalities
import org.apache.spark.rdd.RDD
class MainApp {
  val sales: String = "./files/sales"
  val customer: String = "./files/customer"
  val splitToken: String = "#"
  def main (args: Array[String]): Unit = {
    val operate: Functionalities = Functionalities(customer,sales,splitToken)
    val resultRDD: RDD[(Long, (String, String))] = operate.getCustomerSales

  }
}
