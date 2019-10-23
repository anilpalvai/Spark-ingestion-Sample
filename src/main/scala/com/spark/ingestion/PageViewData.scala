package com.spark.ingestion

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD


object PageViewData {
  def main(args: Array[String]): Unit = {


    lazy val logger = Logger.getLogger(getClass.getName)
    logger.setLevel(Level.WARN)

    logger.info("Building Sparksession created------->>>>>")
    val spark = SparkSession.builder
      .appName("Shopback Spark homework")
      .master("local[*]")
      .getOrCreate

    import spark.implicits.{_}

    val type_name = "pageview"

    logger.info("logging created------->>>>>" + s"extracting the event of $type_name")

    //Reading the parquet data from Input resource folder
  //  val InputParquetPath = this.asInstanceOf[Any].getClass.getResource("/part-00000-035a9461-401f-4115-9f0c-64924620fa3f.snappy.parquet")
    val Sampledata_rdd = spark.read.parquet("/Users/anilpalwai/IdeaProjects/Spark-Scala-Homework/inputs/part-00000-035a9461-401f-4115-9f0c-64924620fa3f.snappy.parquet")
    Sampledata_rdd.write.format("com.databricks.spark.avro").mode().save("/Users/anilpalwai/IdeaProjects/Spark-Scala-Homework/inputs1")
    //INPUT Shema of ParquetFile
    val schema = new StructType()
      .add(StructField("ts", TimestampType, true))
      .add(StructField("id", StringType, true))
      .add(StructField("topic", StringType, true))
      .add(StructField("properties", StringType, true))

    val Sampledata_df = Sampledata_rdd.toDF()
    val PageViewDf = Sampledata_df
                      .filter($"name" === s"$type_name")
    PageViewDf.printSchema();
    PageViewDf.show(10);

    val Pageview_Json_Schema = StructType(Seq(
                                  StructField("pageUrl", StringType, true),
                                  StructField("pageIsFirstEntry", BooleanType, true)
                                ))


    logger.info("Intial Dataframe------->>>>>")
    val ParsedJson = PageViewDf.withColumn("properties", from_json(col("properties"), Pageview_Json_Schema))
    val InitialPageviewDF = ParsedJson.withColumn("pageUrl", $"properties.pageUrl")
                    .withColumn("pageIsFirstEntry", $"properties.pageIsFirstEntry")
                    .select($"profile_id",$"pageUrl",$"pageIsFirstEntry")

    InitialPageviewDF.cache()


    logger.info("Output1------->>>>>")

    //Createing DF with Page_Set column by grouping ProfileId and Pageurl
    val output1 = InitialPageviewDF //.withColumn("page_set", collect_set($"pageUrl"))
                  .groupBy($"profile_id", $"pageIsFirstEntry")
                  .agg(collect_set($"pageUrl").as("page_set"))


    //output1.write.parquet("/output/output1.parquet")
    //Createing DF with Page_Set Count column by grouping ProfileId and Pageurl
    logger.info("Output2------->>>>>")
    val output2: DataFrame = output1.select(col("profile_id"), col("pageIsFirstEntry"), size(col("page_set")).alias("count"))
   // output2.write.parquet("/output/output2.parquet")

    import spark.implicits.{_}
    //val redisdf1 = output2.map(i => println(i.toString()))



    import com.redislabs.provider.redis._

    //Preparing data for Radis key,value pairs
    val redisdf = output2
                  .select(concat_ws("_", $"profile_id", $"pageIsFirstEntry").as("key"), $"count")
    val RadisKV = redisdf.rdd.map(a => (a.getString(0), a.getInt(1).toString))
    RadisKV.take(2)


    //Radis Properties
    val redisServerDnsAddress = "120.0.0.1"
    val redisPortNumber = 6379
    val redisPassword = "REDIS_PASSWORD"
//    val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

    //Writing the Datato Radis as KeyValue Pairs
 //   spark.sparkContext.toRedisKV(RadisKV)(redisConfig)


    //Reqading From RadisKV
 //   val keysRDD = spark.sparkContext.fromRedisKeyPattern("web*")(redisConfig)
  //  val stringRDD = keysRDD.getKV
  //  stringRDD.collect()


  }

}
