package com.github.tootop101

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.NewHadoopRDD

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object SparkThaiLanguageNewApi {
  def main(args: Array[String]) {

    val inputDirectory = args(0)
    val outputDirectory = args(1)
    val sparkConf = new SparkConf().setAppName("SparkThaiLanguageNewApi")

    val sc = new SparkContext(sparkConf)
    val hadoopConf = sc.hadoopConfiguration

    val data = sc.newAPIHadoopFile(inputDirectory, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf)
    val newHadoopRdd = data.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      .map { line => new String(line._2.getBytes, "TIS620") }
      .map { x => x.split("\t").mkString("|") }
      .saveAsTextFile(outputDirectory)
  }
}