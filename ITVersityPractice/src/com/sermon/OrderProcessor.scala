package com.sermon

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OrderProcessor {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    
    //val orders = sc.textFile("/Users/conejitos/marcos/data/orders.txt")
    val orders = sc.textFile("/Users/serranm1/temp/miGIT/pythonPractice/datos/orders.txt")
    
    //val orderRDD = orders.map(x => parseNames(x))
    val orderRDD = orders.map{parseNames}
    
    orderStatusTop100(orderRDD)
    //orderRDD.map( x => (x._2, 1)).reduceByKey( (x, y) => x + y ).sortByKey().foreach(println(_))
    
    println("-------------------------------------------------------------------")
    orderDate(orderRDD)
    
    println("-------------------------------------------------------------------")
    orderByMonth(orderRDD)

    println("-------------------------------------------------------------------")
    orderByYear(orderRDD)
  }
  
  def parseNames(line : String) : (Int, (String, Int, String)) = {
    val fields = line.split(",")
    // order_id -> (order_date, order_customer_id, order_status)
    (fields(0).toInt, (fields(1).toString(), fields(2).toInt, fields(3).toString()))
  }  
  
  def parseDateByYear(line : String) : String = {
    val fields = line.split(" ")
    val fechas = fields(0).split("-")
    fechas(0)
  }
  
  def parseDateByYearMonth(line : String) : String = {
    val fields = line.split(" ")
    val fechas = fields(0).split("-")
    fechas(0) + "-" + fechas(1)
  }
  
  def orderStatusTop100(orderRDD : RDD[(Int, (String, Int, String))]) {
    // map(order_status, 1) -> reduceByKey -> map(counter, order_status) -> sortByKey descending
    val orderStatus = orderRDD.map{x => (x._2._3, 1)}
                              .reduceByKey{ (x, y) => x + y}
                              .map{x => (x._2, x._1)}
                              
    orderStatus.sortByKey(false)
                .foreach{x => println(x._2 + " - " + x._1)}
  }
  
  def orderDate(orderRDD : RDD[(Int, (String, Int, String))]) {
    // map (order_date, 1) -> reduceByKey
    val orderDate = orderRDD.map{x => (x._2._1, 1)}
                            .reduceByKey{ (x, y) => x + y}
                            
    // map ( (order_date, order_status), 1) -> reduceByKey -> map (order_date, (order_status, counter) )
    val orderDateStatus = orderRDD.map{x => ((x._2._1, x._2._3), 1)}
                                  .reduceByKey{ (x, y) => x + y}
                                  .map{x => (x._1._1 , (x._1._2, x._2))}
    
    // join -> filter by 2014-07
    val orderJoin = orderDate.join(orderDateStatus)
                              .filter{x => x._1.toString().contains("2014-07")}
    
    orderJoin.sortByKey(false).foreach{x => println(x._1 + " - " + x._2._2._2 + " - " + x._2._2._1)}
  }
  
  def orderByMonth(orderRDD : RDD[(Int, (String, Int, String))]) {
    // map by YYYY-MM -> reduceByKey
    val orderDate = orderRDD.map{x => (parseDateByYearMonth(x._2._1), 1)}
                            .reduceByKey{_ + _}

    // map by (YYYY-MM, order_status) -> reduceByKey -> map (date, (order_status, counter))                        
    val orderDateStatus = orderRDD.map{x => ((parseDateByYearMonth(x._2._1), x._2._3), 1)}
                                  .reduceByKey{_ + _}
                                  .map{x => (x._1._1, (x._1._2, x._2))}
                                  
    val orderJoin = orderDate.join(orderDateStatus)
    
    orderJoin.sortByKey(false).foreach{x => println(x._1 + "  " + x._2._1 + "  " + x._2._2)}
  }
  
  def orderByYear(orderRDD : RDD[(Int, (String, Int, String))]) {
    // map by YYYY -> reduceByKey
    val orderDate = orderRDD.map{x => (parseDateByYear(x._2._1), 1)}
                            .reduceByKey{_ + _}

    // map by (YYYY, order_status) -> reduceByKey -> map (date, (order_status, counter))                            
    val orderDateStatus = orderRDD.map{x => ((parseDateByYear(x._2._1), x._2._3), 1)}
                                  .reduceByKey{_ + _}
                                  .map{x => (x._1._1, (x._1._2, x._2))}
                                  
    val orderJoin = orderDate.join(orderDateStatus)
    
    orderJoin.sortByKey(false).foreach{x => println(x._1 + "  " + x._2._1 + "  " + x._2._2)}
  }
  
}