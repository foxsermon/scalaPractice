package com.sermon

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object OrderItemsProblem1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val orders = sqlContext.read.format("com.databricks.spark.avro").load("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/problem1/orders/part-m-00000.avro")

    val orderItems = sqlContext.read.format("com.databricks.spark.avro").load("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/problem1/order_items/part-m-00000.avro");
    
    val orderJoin = orders.join(orderItems, orders("order_id") === orderItems("order_item_order_id"))
    
    val orderGroup = orderJoin.groupBy(to_date(from_unixtime(col("order_date") / 1000)).alias("order_date"), col("order_status"))
                              .agg(sum("order_item_subtotal").alias("total_amount"), countDistinct("order_id").alias("total_orders"))
                              .orderBy(desc("order_date"), col("order_status"), desc("total_amount"), col("total_orders"))

    orderGroup.show()
    
    orderJoin.registerTempTable("order_joined");

    val sqlResult = sqlContext.sql("select to_date(from_unixtime(cast(order_date/1000 as bigint))) as order_formatted_date, order_status, " +
									                "cast(sum(order_item_subtotal) as DECIMAL (10,2)) as total_amount, count(distinct(order_id)) as total_orders " +
									                "from order_joined group by to_date(from_unixtime(cast(order_date/1000 as bigint))), order_status " +
									                "order by order_formatted_date desc,order_status,total_amount desc, total_orders");
    sqlResult.show()
    
    /*
    val comByKeyResult = orderJoin.map(x => ( (x(1).toString, x(3).toString), (x(8).toString.toFloat, x(0).toString) ))
                                  .combineByKey(
                                      (x:(Float, String)) => (x._1, Set(x._2)),
                                      (x:(Float, Set[String]), y:(Float, String)) => (x._1 + y._1, x._2 + y._2),
                                      (x:(Float, Set[String]), y:(Float, Set[String])) => (x._1 + y._1, x._2 ++ y._2)
                                  )
                                  .map(x=> (x._1._1, x._1._2, x._2._1, x._2._2.size))
                                  .toDF()
                                  .orderBy(col("_1").desc,col("_2"),col("_3").desc,col("_4"));
		*/
    
    println("C'est fini " + orders.count())
  }
}