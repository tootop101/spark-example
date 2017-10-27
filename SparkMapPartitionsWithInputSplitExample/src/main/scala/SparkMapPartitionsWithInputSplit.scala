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

object SparkMapPartitionsWithInputSplit {
  def main(args: Array[String]) {

    val debug = true
    var inputDirectory: String = ""
    var outputDirectory: String = ""
    val sparkConf = new SparkConf().setAppName("SparkMapPartitionsWithInputSplit")

    val sc = new SparkContext(sparkConf)

	val hadoopConf = sc.hadoopConfiguration
	
    val text = sc.newAPIHadoopFile(inputDirectory, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf)
    val newHadoopRdd = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
    val transaction = newHadoopRdd.mapPartitionsWithInputSplit { (inputSplit, iterator) =>
      val file = inputSplit.asInstanceOf[FileSplit]
      val fs = FileSystem.get(hadoopConf)
      val fileName = file.getPath.getName
      val modifiyDatetimeLong = fs.getFileStatus(file.getPath).getModificationTime()
      val date = new java.util.Date(modifiyDatetimeLong)
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val modifiyDatetime = dateFormat.format(date)
      iterator.map { x => (fileName, modifiyDatetime, x._2.toString()) }
    }.cache()
  }

}