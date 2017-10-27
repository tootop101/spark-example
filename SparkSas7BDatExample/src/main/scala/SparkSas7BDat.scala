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
    .setMaster("local")
    .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(sparkConf)
    //sc.setLogLevel("DEBUG")
    val sqlContext = new SQLContext(sc)
    //sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    val sas_data = sqlContext.read.format("com.github.saurfang.sas.spark").load(sasfile).cache()
    
    sas_data.write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("hello_csv")

  }
}