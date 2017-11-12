package com.github.tootop101

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import com.databricks.spark.csv._
import com.github.saurfang.sas.spark._

object SparkSas7BDat {
  def main(args: Array[String]) {

    val sasfile = "world_cities.sas7bdat"
    
    val sparkConf = new SparkConf().setAppName("SparkSas7BDat")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val sas_data = sqlContext.read.format("com.github.saurfang.sas.spark").load(sasfile).cache()
    
    sas_data.write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("hello_csv")

  }
}