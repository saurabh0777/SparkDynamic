
package com.test.poc
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object readParquet {
 
   def main(args: Array[String]) = {
    // Two threads local[2]
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ParquetTest")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    readParquet(sqlContext)
  }

   def readParquet(sqlContext: SQLContext) = {
    // read back parquet to DF
    val newDataDF = sqlContext.read.parquet("D://Data//output//*")
    // show contents
    newDataDF.show()
  }
  
}
