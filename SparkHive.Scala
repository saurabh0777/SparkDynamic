package com.examples

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import java.util.Properties

object RunEtl  {

  def main(args:Array[String]):Unit ={

    val connectionProperties = new Properties()
    connectionProperties.put("user", "bigdatadev")
    connectionProperties.put("password", "MSPuT")

   

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local")
      .config("ConnectionUserName", "bigdatadev")
      .config("ConnectionPassword","MSa4d!$")
      .config("hive.metastore.warehouse.uris","thrift://SGBRTSDPOC121.com.my:9083")

      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .enableHiveSupport()
      .getOrCreate()

    val listdb =  spark.sqlContext.sql("select count(*) from a20180721")

    listdb.show()

    /*  if (args.length<1){
        log.error("Insufficient Arguments: <classNames comma separated>")
        System.exit(1)

      }*/

   
  }

}
