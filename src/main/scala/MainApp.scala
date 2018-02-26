import Operations.Functionalities

class MainApp {
  val sales: String = "./files/sales"
  val customer: String = "./files/customer"
  val splitToken: String = "#"
  def main (args: Array[String]): Unit = {
    val operate: Functionalities = Functionalities(customer,sales,splitToken)
    operate.result(operate.getCustomerSales)
    operate.sparkContext.stop()
  }
}
