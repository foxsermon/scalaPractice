package com.sermon

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import scala.collection.Set
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.FloatType

object OrderItemsProblem1 {
  
  def dfSchema(): StructType = { 
    StructType( 
        Seq( 
            StructField(name = "order_date", dataType = StringType, nullable = false),
            StructField(name = "order_status", dataType = StringType, nullable = false), 
            StructField(name = "order_total", dataType = FloatType, nullable = false), 
            StructField(name = "order_id_total", dataType = IntegerType, nullable = false) 
           ) 
     ) 
  }

  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val orders = sqlContext.read.format("com.databricks.spark.avro").load("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/problem1/orders/part-m-00000.avro")

    val orderItems = sqlContext.read.format("com.databricks.spark.avro").load("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/problem1/order_items/part-m-00000.avro");
    
    val orderJoin = orders.join(orderItems, orders("order_id") === orderItems("order_item_order_id"))
    
    val dataFrameResult = orderJoin.groupBy(to_date(from_unixtime(col("order_date") / 1000)).alias("order_date"), col("order_status"))
                              .agg(sum("order_item_subtotal").alias("total_amount"), countDistinct("order_id").alias("total_orders"))
                              .orderBy(desc("order_date"), col("order_status"), desc("total_amount"), col("total_orders"))

    dataFrameResult.show()
    println("-------------------------------------------------------------------------------")
    
    orderJoin.registerTempTable("order_joined");

    val sqlResult = sqlContext.sql("select to_date(from_unixtime(cast(order_date/1000 as bigint))) as order_formatted_date, order_status, " +
									                "cast(sum(order_item_subtotal) as DECIMAL (10,2)) as total_amount, count(distinct(order_id)) as total_orders " +
									                "from order_joined group by to_date(from_unixtime(cast(order_date/1000 as bigint))), order_status " +
									                "order by order_formatted_date desc,order_status,total_amount desc, total_orders");
    sqlResult.show()
    println("-------------------------------------------------------------------------------")
    
    // map ( (order_date, order_status) , (price, order_id) )
    val orderRows = orderJoin.rdd.map(x => ((x(1).toString(), x(3).toString()), (x(8).toString().toFloat, x(0).toString())))
                                  .combineByKey(
                                       (x : (Float, String)) => (x._1, Set(x._2)),
                                       (x : (Float, Set[String]), y : (Float, String)) => (x._1 + y._1, x._2 + y._2), 
                                       (x : (Float, Set[String]), y : (Float, Set[String])) => (x._1 + y._1, x._2 ++ y._2)
                                      )
                                  .map(x => Row(x._1._1, x._1._2, x._2._1, x._2._2.size))
   val mySchema = dfSchema()                                  
   val combByKeyResult = sqlContext.createDataFrame(orderRows, mySchema)
                                  .orderBy(desc("order_date"), col("order_status"), desc("order_total"), col("order_id_total"))

   combByKeyResult.show()
   
   
   println("-----------------------------------------------------------------------------------------------------------------")
   println("Save result as CVS")

   dataFrameResult.rdd.coalesce(1, true).map(x => x(0).toString() + "," + x(1).toString() + "," + x(2).toString() + "," + x(3).toString()).saveAsTextFile("dataFrameResult.cvs")
   sqlResult.rdd.coalesce(1, true).map(x => x(0).toString() + "," + x(1).toString() + "," + x(2).toString() + "," + x(3).toString()).saveAsTextFile("sqlResult.cvs")
   combByKeyResult.rdd.coalesce(1, true).map(x => x(0).toString() + "," + x(1).toString() + "," + x(2).toString() + "," + x(3).toString()).saveAsTextFile("comByKeyResult.cvs")

   println("-----------------------------------------------------------------------------------------------------------------")
   println("Compress result as GZIP")

   sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
   dataFrameResult.coalesce(1).write.mode("overwrite").parquet("dataFrameResult.gzip")
   sqlResult.coalesce(1).write.mode("overwrite").parquet("sqlResult.gzip")
   combByKeyResult.coalesce(1).write.mode("overwrite").parquet("combByKeyResult.gzip")

   println("-----------------------------------------------------------------------------------------------------------------")
   println("Compress result as SNAPPY")
   
   sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
   dataFrameResult.coalesce(1).write.mode("overwrite").parquet("dataFrameResult-snappy")
   sqlResult.coalesce(1).write.mode("overwrite").parquet("sqlResult-snappy")
   combByKeyResult.coalesce(1).write.mode("overwrite").parquet("combByKeyResult-snappy")

   println("-----------------------------------------------------------------------------------------------------------------")

   
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