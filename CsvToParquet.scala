package com.maxis.poc
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._


object ParquetConverter {
  
  val logger: Logger = LogManager.getLogger(getClass)
  
   def main(args: Array[String]) = {
   

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ParquetTest")
    val sc: SparkContext = new SparkContext(conf)
     sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    val sqlContext: SQLContext = new SQLContext(sc)
    writeparquet(sc, sqlContext)
 
  }
    
  def writeparquet(sc: SparkContext, sqlContext: SQLContext) = {
    // Read file as RDD
    // Specify the custom header as per need.
   /* val customSchema = StructType(Array(
        StructField("event_timestamp", StringType, true),
        StructField("bearer_imsi", StringType, true),
        StructField("app_category", StringType, true),
        StructField("app_name", StringType, true),
        StructField("cell_site", StringType, true),
        StructField("bearer_cell", StringType, true),
        StructField("bearer_apn", StringType, true),
        StructField("imei_tac", StringType, true),
        StructField("http_host", StringType, true),
        StructField("kpi_total_application_volume_uplink_and_downlink", StringType, true)
      ))*/
    
    val rdd = sqlContext.read.format("csv")/*.schema(customSchema)*/.option("header", "true").load("D:/Saurabh/android_new.csv")
    // Convert rdd to data frame using toDF; the following import is required to use toDF function.
      println(rdd.printSchema())
     
       val df: DataFrame = rdd.toDF()
    
    // Write file to parquet
    df.coalesce(1).write.mode("append").parquet("D:/Saurabh/output")
    df.show(4)
    df.count()
    sc.stop()
  }
     
  
}


    
