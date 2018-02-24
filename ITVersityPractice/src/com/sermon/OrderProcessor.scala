package com.sermon

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object OrderProcessor {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    
    val orders = sc.textFile("/Users/serranm1/temp/miGIT/pythonPractice/datos/orders.txt")
    
    // orders.map(x => (x(0), 1)).foreach(x => println(x))
    
    //val orderRDD = orders.map(x => parseNames(x))
    val orderRDD = orders.map(parseNames)
    println("Counter => " + orderRDD.count())
    
    //orderStatusTop100(orderRDD)
    orderRDD.map( (x) => new Tuple2(x, 1)).foreach(println(_))
    
    println("Salio")
  }
  
  def parseNames(line : String) {
    val fields = line.split(',')
    return new Tuple2(fields(0).toInt, fields(1).toString())
  }
  
  def orderStatusTop100(orderRDD : RDD[Unit]) {
    val orderStatus = orderRDD.map(x => (x.->(1), 1)).foreach(x => println(x._1))
  }
}