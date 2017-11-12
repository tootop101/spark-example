package com.github.tootop101

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextInputFormat

object SparkThaiLanguage {
  def main(args: Array[String]) {

    val inputDirectory = args(0)
    val outputDirectory = args(1)
    val sparkConf = new SparkConf().setAppName("SparkThaiLanguage")

    val sc = new SparkContext(sparkConf)

    val data = sc.hadoopFile[LongWritable, Text, TextInputFormat](inputDirectory)
      .map { line => new String(line._2.getBytes, "TIS620") }
      .map { x => x.split("\t").mkString("|") }
      .saveAsTextFile(outputDirectory)
  }
}