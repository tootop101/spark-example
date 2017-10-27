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

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit

import org.apache.spark.rdd.NewHadoopRDD

object SparkThaiLanguageNewApi {
  def main(args: Array[String]) {

    val debug = true
    var inputDirectory: String = ""
    var outputDirectory: String = ""
    val sparkConf = new SparkConf().setAppName("SparkThaiLanguageNewApi")

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

	val hadoopConf = sc.hadoopConfiguration
	
    val data = sc.newAPIHadoopFile(inputDirectory, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf)
	val newHadoopRdd = data.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      .map { line => new String(line._2.getBytes, "TIS620") }
      .filter { x => !x.matches(regex) }
      //.map { x => x.split("\t").mkString("|") }
      .saveAsTextFile(outputDirectory)
  }

}