package com.markgrover.spark
import java.nio.ByteBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Allocates as many buffers as are words in the provided file.
// Capacity is the second argument and is optional, defaulting to 100
object LotsOfByteBuffers {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Lots of ByteBuffers"))
    val split = sc.textFile(args(0)).flatMap(_.split(" "))
    val capacity = if (args.length > 1) args(1).toInt else 100

    split.map(element => System.out.println(s"Allocating 1 buffer of $capacity capacity"); ByteBuffer.allocateDirect(capacity))
  }
}
