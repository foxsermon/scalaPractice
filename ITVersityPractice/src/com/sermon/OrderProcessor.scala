package com.sermon

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OrderProcessor {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    
    val orders = sc.textFile("/Users/conejitos/marcos/data/orders.txt")
    /*  
    val orderRDD = orders.map{t =>                
      val p = t.split(",")
      (p(0).toInt, p(1).toString())
    }
    */

    // orders.map(x => (x(0), 1)).foreach(x => println(x))
    
    //val orderRDD = orders.map(x => parseNames(x))
    val orderRDD = orders.map{parseNames}
    
    orderStatusTop100(orderRDD)
    //orderRDD.map( x => (x._2, 1)).reduceByKey( (x, y) => x + y ).sortByKey().foreach(println(_))
    
    println("-------------------------------------------------------------------")
    orderDate(orderRDD)
  }
  
  def parseNames(line : String) : (Int, (String, Int, String)) = {
    val fields = line.split(",")
    (fields(0).toInt, (fields(1).toString(), fields(2).toInt, fields(3).toString()))
  }
  
  def orderStatusTop100(orderRDD : RDD[(Int, (String, Int, String))]) {
    //val orderStatus = orderRDD.map(x => (x.->(1), 1)).foreach(x => println(x._1))
    val orderStatus = orderRDD.map{x => (x._2._3, 1)}
                              .reduceByKey{ (x, y) => x + y}
                              .map{x => (x._2, x._1)}
                              
    orderStatus.sortByKey(false)
                .foreach{x => println(x._2 + " - " + x._1)}
  }
  
  def orderDate(orderRDD : RDD[(Int, (String, Int, String))]) {
    val orderDate = orderRDD.map{x => (x._2._1, 1)}
                            .reduceByKey{ (x, y) => x + y}
    
    val orderDateStatus = orderRDD.map{x => ((x._2._1, x._2._3), 1)}
                                  .reduceByKey{ (x, y) => x + y}
                                  .map{x => (x._1._1 , (x._1._2, x._2))}
    
    val orderJoin = orderDate.join(orderDateStatus)
                              .filter{x => x._1.toString().contains("2014-07")}
    
    orderJoin.sortByKey(false).foreach{x => println(x._1 + " - " + x._2._2._2 + " - " + x._2._2._1)}
  }
}