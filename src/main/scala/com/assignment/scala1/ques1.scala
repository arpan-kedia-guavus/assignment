package com.assignment.scala1

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


 object ques1 {

  def main(args: Array[String]): Unit ={


    val sparkSession = SparkSession
      .builder()
      //      .master("local[*]")
      .appName("Arpan_Assinment")
      .config("hive.metastore.uris","thrift://jiouapp001-mst-01.gvs.ggn:9083")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    //-----------------------------------------------------------------------------------------------------------------------------------

   // var df = sparkSession.read.format("csv").option("header", "true").load("/data/arpan/data/newcsvfile.csv")
      var df = sparkSession.read.format("csv").option("header", "true").load("/data/arpan/data1/edr_dataset_final1.csv")
    df = df.withColumn("hour",substring(col("sn-start-time"),12,2)).alias("hour").withColumn("minute",substring(col("sn-start-time"),15,2)).alias("minute")
    var renamedDataFrame = df

    for (field <- renamedDataFrame.columns) {
      renamedDataFrame = renamedDataFrame
        .withColumnRenamed(field, field.replaceAll("-", "_").replaceAll(" ","_"))}
    renamedDataFrame = renamedDataFrame.withColumnRenamed("#sn_end_time","sn_end_time")
    df = renamedDataFrame
    df.write.mode("overwrite").partitionBy("hour","minute").format("orc").saveAsTable("arpan_test.partitioned_source_table")


    //Ques-1_1
    sparkSession.sql("drop table if exists arpan_test.top_10_customer")
    var ques1 = df.groupBy("radius_user_name","hour").agg(sum(col("transaction_uplink_bytes")).alias("upload_bytes"),first(col("sn_start_time")).alias("sn_start_time"))
    val window = org.apache.spark.sql.expressions.Window.partitionBy("hour").orderBy(desc("upload_bytes"))
    val df2 =  ques1.withColumn("row_num", rank() over (window)).filter(col("row_num") <= 10).withColumn("hour",substring(col("sn_start_time"),12,2)).withColumn("basis_of", lit("upload_bytes") )
    df2.orderBy(desc("upload_bytes")).groupBy("hour")
    df2.select(col("radius_user_name"),col("sn_start_time"),col("hour"),col("upload_bytes").alias("Total_Bytes"),col("basis_of")).write.mode("append").partitionBy("hour").format("orc").saveAsTable("arpan_test.top_10_customer")

    //Ques-1_2
    var ques12 = df.groupBy("radius_user_name","hour").agg(sum(col("transaction_downlink_bytes")).alias("download_bytes"),first(col("sn_start_time")).alias("sn_start_time"))
    val window_1 = org.apache.spark.sql.expressions.Window.partitionBy("hour").orderBy(desc("download_bytes"))
    val df22 =  ques12.withColumn("row_num", rank() over (window_1)).filter(col("row_num") <= 10).withColumn("hour",substring(col("sn_start_time"),12,2)).withColumn("basis_of", lit("download_bytes") )
    df22.orderBy(desc("download_bytes")).groupBy("hour")
    df22.select(col("radius_user_name"),col("sn_start_time"),col("hour"),col("download_bytes").alias("Total_Bytes"),col("basis_of")).write.mode("append").partitionBy("hour").format("orc").saveAsTable("arpan_test.top_10_customer")

    //Ques-1_3
    var ques13 = df.groupBy("radius_user_name","hour").agg((sum(col("transaction_uplink_bytes")) + sum(col("transaction_downlink_bytes"))).alias("Tonnage"),first(col("sn_start_time")).alias("sn_start_time"))
    val window_2 = org.apache.spark.sql.expressions.Window.partitionBy("hour").orderBy(desc("Tonnage"))
    val df23 =  ques13.withColumn("row_num", rank() over (window_2)).filter(col("row_num") <= 10).withColumn("hour",substring(col("sn_start_time"),12,2)).withColumn("basis_of", lit("tonnage") )
    df23.orderBy(desc("Tonnage")).groupBy("hour")
    df23.select(col("radius_user_name"),col("sn_start_time"),col("hour"),col("Tonnage").alias("Total_Bytes"),col("basis_of")).write.mode("append").partitionBy("hour").format("orc").saveAsTable("arpan_test.top_10_customer")



  }





}
