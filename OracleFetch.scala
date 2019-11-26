package com.test.poc

import org.apache.spark.sql.SparkSession

    object PullOrcale {
      
  def main(args: Array[String]): Unit = {
      
    val spark = SparkSession.builder()
          .appName("ConnectingOracleDatabase")
          .master("local[*]")
          .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.100.65.136:1521/CVMDBP")
      .option("dbtable", "MINING.CAR_PR_SUB_CSV_20181031")
      .option("user", "XYZ")
      .option("password", "FAU#")
      .option("driver", "oracle.jdbc.OracleDriver")
      .load()
      
    jdbcDF.printSchema()

    jdbcDF.write.mode("overwrite").option("header", "true").format("csv").save("D:/Data/output/CAR_PR_SUB_CSV_20181031/")
    
    
    /*jdbcDF.coalesce(1).write.mode("append").parquet("D:/Data/output/CAR_PO_SUB_CSV_20181031")
*/
  }
}
