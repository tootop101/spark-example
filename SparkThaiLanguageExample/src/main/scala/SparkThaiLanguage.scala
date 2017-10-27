package com.github.tootop101

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextInputFormat

object SparkThaiLanguage {
  def main(args: Array[String]) {

    val debug = true
    var inputDirectory: String = ""
    var outputDirectory: String = ""
    val sparkConf = new SparkConf().setAppName("SparkThaiLanguage")

    if (debug) {
      inputDirectory = "/home/test/payment.txt"
      outputDirectory = "output"
      sparkConf.setMaster("local[*]")
    } else {
      inputDirectory = args(0)
      outputDirectory = args(1)
    }

    val sc = new SparkContext(sparkConf)
    val regex = "^\\D.*$"

    val data = sc.hadoopFile[LongWritable, Text, TextInputFormat](inputDirectory)
      .map { line => new String(line._2.getBytes, "TIS620") }
      .filter { x => !x.matches(regex) }
      //.map { x => x.split("\t").mkString("|") }
      .saveAsTextFile(outputDirectory)
  }

}