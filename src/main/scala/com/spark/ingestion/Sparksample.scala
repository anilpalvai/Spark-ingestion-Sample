package com.spark.ingestion

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Sparksample {

  lazy val logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.WARN)

  logger.info("Building Sparksession created------->>>>>")
  val spark = SparkSession.builder
    .appName("Shopback Sparksample")
    .master("local[*]")
    .getOrCreate
  import spark.implicits.{_}
  spark.read.format("com.databricks.spark.avro")
    .load()
}
