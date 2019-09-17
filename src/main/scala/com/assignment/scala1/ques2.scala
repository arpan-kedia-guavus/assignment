package com.assignment.scala1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, first, rank, sum, udf, when}

object ques2 {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession
      .builder()
      //      .master("local[*]")
      .appName("Arpan_Assinment")
      .config("hive.metastore.uris", "thrift://jiouapp001-mst-01.gvs.ggn:9083")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    def content_type (s: String) : String = {
      if (s.split("/").length > 1) {
        var part = s.split("/")(0).toLowerCase()
        if (part.contains("application") || part.contains("multipart")) {
          if(part.contains("multipart")){
            "multipart"
          }
          else{
            var part1 = s.split("/")(1).toLowerCase()
            if(part1.contains("json"))
              "json"
            else if(part1.contains("text"))
              "text"
            else if(part1.contains("xml"))
              "xml"
            else if(part1.contains("pdf"))
              "pdf"
            else if(part1.contains("stream"))
              "stream"
            else if(part1.contains("javascript"))
              "javascript"
            else
              "unknown"
          }
        }
        else
          part
      }
      else
        ""
    }


    val splitting_content = udf[String,String](content_type)
    val df = sparkSession.table("arpan_test.partitioned_source_table")


    val content_typeConditionCheck = col("http_content_type").isNull.or(col("http_content_type").===(""))
    var temp1 = df.withColumn("http_content",when(content_typeConditionCheck,"").otherwise(splitting_content(col("http_content_type"))))
    var temp3 = temp1.groupBy("radius_user_name","http_content").agg(count(col("radius_user_name")).alias("count_of_hits"),first(col("http_reply_code")).alias("http_reply_code")).filter(col("http_reply_code").substr(0,1) === "2")
    val window1 = org.apache.spark.sql.expressions.Window.partitionBy("radius_user_name").orderBy(desc("count_of_hits"))
    val df3 =  temp3.withColumn("row_num", rank() over (window1)).filter(col("row_num") <= 5)
    var finalResult = df3.select(col("radius_user_name"),col("http_content"),col("count_of_hits"),col("row_num")).orderBy(col("radius_user_name"),desc("count_of_hits"))

    var temp3_1 = temp1.groupBy("radius_user_name","http_content").agg(sum("transaction_downlink_bytes").alias("bytes_downloaded"))
    val window1_1 = org.apache.spark.sql.expressions.Window.partitionBy("radius_user_name").orderBy(desc("bytes_downloaded"))
    val df3_1 =  temp3_1.withColumn("row_num", rank() over (window1_1)).filter(col("row_num") <= 5)
    var finalResult1 = df3_1.select(col("radius_user_name").alias("radius_user_name_1"),col("http_content").alias("http_content_1"),col("bytes_downloaded"),col("row_num").alias("row_num_1")).orderBy(col("radius_user_name"),desc("bytes_downloaded"))

    var temp3_2 = temp1.groupBy("radius_user_name","http_content").agg(sum("transaction_uplink_bytes").alias("bytes_uploaded"))
    val window1_2 = org.apache.spark.sql.expressions.Window.partitionBy("radius_user_name").orderBy(desc("bytes_uploaded"))
    val df3_2 =  temp3_2.withColumn("row_num", rank() over (window1_2)).filter(col("row_num") <= 5)
    var finalResult2 = df3_2.select(col("radius_user_name").alias("radius_user_name_2"),col("http_content").alias("http_content_2"),col("bytes_uploaded"),col("row_num").alias("row_num_2")).orderBy(col("radius_user_name"),desc("bytes_uploaded"))

    var finaltable1 = finalResult.join(finalResult1,finalResult.col("radius_user_name") === finalResult1.col("radius_user_name_1") and(finalResult.col("row_num") === finalResult1.col("row_num_1")),"left").join(finalResult2,finalResult.col("radius_user_name") === finalResult2.col("radius_user_name_2") and(finalResult.col("row_num") === finalResult2.col("row_num_2")),"left").orderBy(finalResult.col("radius_user_name"),finalResult.col("row_num"))
    finaltable1.select(col("radius_user_name"),col("http_content").alias("http_content_hits"),col("count_of_hits"),col("http_content_1").alias("http_content_downloads"),col("bytes_downloaded"),col("http_content_2").alias("http_content_uploads"),col("bytes_uploaded")).write.mode("overwrite").format("orc").saveAsTable("arpan_test.ques_2")



  }
}
