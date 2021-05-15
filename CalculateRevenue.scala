// Databricks notebook source
// MAGIC %fs ls /FileStore/tables

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType, StringType, FloatType, StructType, StructField}

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

val ordersItemsSchema = new StructType()
  .add($"order_item_id".int)
  .add($"order_item_order_id".string)
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
    StructField("customer_zipcode", StringType),
    )
)


val categories = spark.read
            .schema("category_id INT, category_department_id INT, category_name STRING")
            .csv("dbfs:/FileStore/tables/retaildb/categories/part_00000")

val ordersItems = spark.read
                .schema(ordersItemsSchema)
                .csv("dbfs:/FileStore/tables/orders-items/part_00000")

val orders = spark.read
              .schema(ordersSchema)
              .csv("dbfs:/FileStore/tables/orders/part_00000")

val customers = spark.read
              .schema(customersSchema)
              .csv("dbfs:/FileStore/tables/retaildb/customers/part_00000")

val products = spark.read
              .schema(productSchema)
              .csv("dbfs:/FileStore/tables/products/part_00000")

// COMMAND ----------

categories.printSchema
customers.printSchema
orders.printSchema
ordersItems.printSchema
products.printSchema

// COMMAND ----------

val filteredOrders = orders.filter("order_status IN ('COMPLETE', 'CLOSED', 'PENDING')")

// COMMAND ----------

val joinResult = filteredOrders
        .join(ordersItems, ordersItems("order_item_order_id") === orders("order_id"))
        .join(customers, customers("customer_id") === orders("order_customer_id"))
        .join(products, products("product_id") === ordersItems("order_item_product_id"))
        .select("customer_fname", "customer_lname", "order_date", "order_item_subtotal")


// COMMAND ----------

import org.apache.spark.sql.functions.{concat, lit, date_format, sum, round, asc, desc}

val concattedCustomerName = joinResult
      .select(
        concat($"customer_fname", lit(" "), $"customer_lname").as("name"),
        date_format($"order_date", "yyyyMM").as("month"),
        $"order_item_subtotal"
      )

val result = concattedCustomerName.groupBy("month", "name")
          .agg(round(sum("order_item_subtotal"), 2).as("revenue"))
          .orderBy(asc("month"), desc("revenue"))


// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 2)
result.write.mode("overwrite").csv("/FileStore/tables/retaildb/monthlyRevenue")
