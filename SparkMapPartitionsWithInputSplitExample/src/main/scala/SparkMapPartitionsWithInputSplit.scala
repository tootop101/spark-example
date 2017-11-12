package com.github.tootop101

import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.SparkConf

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.fs.FileSystem

object SparkMapPartitionsWithInputSplit {
  def main(args: Array[String]) {

    val debug = true
    var inputDirectory = args(0)
    var outputDirectory = args(1)

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
      val modifyDatetime = dateFormat.format(date)
      iterator.map { x => (fileName, modifyDatetime, x._2.toString()) }
    }.cache()
  }

}