package com.com.spark

import org.apache.spark.{SparkConfi, SparkContext}

object WordCountExample{

  def main(args: Array[String]): Unit = {

    val session = SparkSession.getOrCreate()

    val sc = session.SparkContext

    val textFile = sc.textFile("/data/read")

    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)

  }
}
