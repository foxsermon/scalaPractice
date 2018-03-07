package com.sermon

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object ProductsProblem2 {
  def dfSchema(): StructType = { 
    StructType( 
        Seq( 
            StructField(name = "productID", dataType = IntegerType, nullable = false),
            StructField(name = "productCatID", dataType = IntegerType, nullable = false),
            StructField(name = "productName", dataType = StringType, nullable = false),
            StructField(name = "productDesc", dataType = StringType, nullable = false), 
            StructField(name = "productPrice", dataType = FloatType, nullable = false),
            StructField(name = "productImage", dataType = StringType, nullable = false)
           ) 
     ) 
  }
  
  def dfSchemaRDDResults(): StructType = { 
    StructType( 
        Seq( 
            StructField(name = "productCatID", dataType = IntegerType, nullable = false),
            StructField(name = "max_price", dataType = FloatType, nullable = false),
            StructField(name = "tot_products", dataType = IntegerType, nullable = false),
            StructField(name = "avg_price", dataType = FloatType, nullable = false), 
            StructField(name = "min_price", dataType = FloatType, nullable = false)
           ) 
     ) 
  }
  
  def parseFile(line : String) : Row = {
    val fields = line.split("\\|")
    Row(fields(0).toInt, fields(1).toInt, fields(2).toString(), fields(3).toString(), fields(4).toFloat, fields(5).toString())
  }  

  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val products = sc.textFile("/Users/serranm1/temp/miGIT/CCA-175/dataPractice/problem2/products/part-m-00000").map{parseFile}

    val productSchema = dfSchema()
    val productsDF = sqlContext.createDataFrame(products, productSchema)
    
    val dataFrameResult = productsDF.filter("productPrice < 100")
                                    .groupBy(col("productCatID"))
                                    .agg(
                                          max("productPrice").alias("max_price"),
                                          countDistinct("productID").alias("tot_products"),
                                          round(avg("productPrice"), 2).alias("avg_price"),
                                          min("productPrice").alias("min_price")
                                        )
                                    .orderBy("productCatID")
    
    dataFrameResult.show()
    println("-----------------------------------------------------------------------------------------------------------------")
    
    productsDF.registerTempTable("products")
    val sqlResult = sqlContext.sql("select productCatID, max(productPrice) as maximum_price,"
									+ " count(distinct(productId)) as total_products,"
									+ " cast(avg(productPrice) as decimal(10, 2)) as average_price,"
									+ " min(productPrice) as minimum_price"
									+ " from products"
									+ " where productPrice < 100"
									+ " group by productCatId"
									+ " order by productCatId")
    sqlResult.show()
    
    println("-----------------------------------------------------------------------------------------------------------------")
    
    // filter by productPrice < 100 -> map (productCatId, (productPrice, productId))
    // -> aggregateByKey by productCatId - max(productPrice), min(productPrice), total ProductPrice, total num of ProductId
    // -> map (productCatId, maxProductPrice, minProductPrice, avgProductPrice, total num of ProductId
    // -> sortby productCatId
    val rddResult = productsDF.rdd.filter(x => (x.get(4).toString().toFloat < 100))
                                  .map(x => (x.get(1).toString().toInt, (x.get(4).toString().toFloat, x.get(0).toString().toInt) ))
                                  .aggregateByKey( (0.0, 9999999.0, 0.0, 0) )(
                                                      (x, y) => (math.max(x._1, y._1), math.min(x._2, y._1), x._3 + y._1, x._4 + 1),
                                                      (x, y) => (math.max(x._1, y._1), math.min(x._2, y._2), x._3 + y._3, x._4 + y._4)
                                                    )
                                  .map(x => (x._1, x._2._1.toFloat, x._2._4, (x._2._3 / x._2._4).toFloat, x._2._2.toFloat))
                                  .sortBy(_._1, true)
    
    //rddResult.foreach(println)
    val rddResultRow = rddResult.map(x => Row(x._1, x._2, x._3, x._4, x._5))
    val schemaRDDResults = dfSchemaRDDResults()									
    val rddResultDF = sqlContext.createDataFrame(rddResultRow, schemaRDDResults)
    
    rddResultDF.show()

    println("-----------------------------------------------------------------------------------------------------------------")
    println("Compress result as SNAPPY")
    
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    dataFrameResult.coalesce(1).write.mode("overwrite").parquet("dataFrameResult-snappy")
    sqlResult.coalesce(1).write.mode("overwrite").parquet("sqlResult-snappy")
    rddResultDF.coalesce(1).write.mode("overwrite").parquet("combByKeyResult-snappy")

    println("C'est fini ")
 
  }
}
