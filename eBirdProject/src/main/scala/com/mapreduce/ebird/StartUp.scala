package com.mapreduce.ebird

import org.apache.spark.{SparkConf, SparkContext}

object StartUp {
  def main(args: Array[String]){
    val conf = new SparkConf()
                  .setAppName("eBird Project")
                    .setMaster("local[*]")

    val sc = new SparkContext(conf)


    sc.stop()
  }
}
