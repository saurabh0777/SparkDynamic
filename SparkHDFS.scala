package com.test.utils;

import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.sql.{DataFrame, SQLContext};

object ReadHDFS {

  def main(arg : Array[String]){
    // Two threads local[2]
    val conf: SparkConf = new SparkConf().setMaster(master = "Looal").setAppName("ParquetTest")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    writeParquet(sc, sqlContext)
    // readParquet(sqlContext)
  }

  def writeParquet(sc: SparkContext, sqlContext: SQLContext) = {
    // Read file as RDD
    val rdd = sqlContext.read.format(source ="csv").option("header", "true").load(path ="hdfs://SGBBDCDPOC0123.men.com.my:8020/data/okla/txt/okala_main_all.csv")
    // Convert rdd to data frame using toDF; the following import is required to use toDF function.
    val df: DataFrame = rdd.toDF()
    // Write file to parquet
    df.write.parquet("hdfs://SGBBDCDPOC0123.men.max.com.my:8020/tmp/android_okla1")

    //How to delete the file in hdfs
    // val fs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://SGBBDPOC01.men.com.my:8020"), sc.hadoopConfiguration)
    // fs.delete(new org.apache.hadoop.fs.Path("/tmp/Employee"),true)
  }


  def readParquet(sqlContext: SQLContext) = {
    // read back parquet to DF
    val rdd = sqlContext.read.format("csv").option("header", "true").load(path ="hdfs://SGBBDCDPOC0123.men.com.my:8020/data/okla/txt/okala_main_all.csv")

    //    val newDataDF = sqlContext.read.parquet("hdfs://SGBBDCDPOC01.men.max.com.my:50470/Sales.parquet")
    // show contents
    rdd.show()

  }
}
