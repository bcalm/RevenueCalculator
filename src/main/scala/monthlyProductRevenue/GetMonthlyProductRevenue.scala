package monthlyProductRevenue
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GetMonthlyProductRevenue {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MonthlyProductRevenueCalculator")
      .master("local")
      .getOrCreate()

    val productSchema = new StructType()
      .add("product_id", IntegerType)
      .add("product_category_id", IntegerType)
      .add("product_name", StringType)
      .add("product_description", StringType)
      .add("product_price", FloatType)
      .add("product_image", StringType)

    val ordersSchema = new StructType()
      .add("order_id", "int")
      .add("order_date", "timestamp")
      .add("order_customer_id", "int")
      .add("order_status", "string")

    import spark.implicits._
    val ordersItemsSchema = new StructType()
      .add($"order_item_id".int)
      .add($"order_item_order_id".int)
      .add($"order_item_product_id".int)
      .add($"order_item_quantity".int)
      .add($"order_item_subtotal".float)
      .add($"order_item_product_price".float)

    val customersSchema = StructType(
      List(
        StructField("customer_id", IntegerType),
        StructField("customer_fname", StringType),
        StructField("customer_lname", StringType),
        StructField("customer_email", StringType),
        StructField("customer_password", StringType),
        StructField("customer_street", StringType),
        StructField("customer_city", StringType),
        StructField("customer_state", StringType),
        StructField("customer_zipcode", StringType)
      )
    )

    val ordersItems = spark.read
      .schema(ordersItemsSchema)
      .csv("retail_db/order_items/part-00000")

    val orders = spark.read
      .schema(ordersSchema)
      .csv("retail_db/orders/part-00000")

    val customers = spark.read
      .schema(customersSchema)
      .csv("retail_db/customers/part-00000")

    val products = spark.read
      .schema(productSchema)
      .csv("retail_db/products/part-00000")

    val filteredOrders = orders.filter("order_status IN ('COMPLETE', 'CLOSED', 'PENDING')")
    
    val joinResult : DataFrame = filteredOrders
      .join(ordersItems, filteredOrders("order_id") === ordersItems("order_item_order_id"))
      .join(customers, filteredOrders.col("order_customer_id") === customers.col("customer_id") )
      .select("customer_fname", "customer_lname", "order_date", "order_item_subtotal")

    val concattedCustomerName = joinResult
      .select(
        concat($"customer_fname", lit(" "), $"customer_lname").as("name"),
        date_format($"order_date", "yyyyMM").as("month"),
        $"order_item_subtotal"
      )

    val result = concattedCustomerName.groupBy("month", "name")
      .agg(round(sum("order_item_subtotal"), 2).as("revenue"))
      .orderBy(asc("month"),asc("revenue"))

    result.write.mode("overwrite").csv("retail_db/monthlyRevenue")
  }
}
