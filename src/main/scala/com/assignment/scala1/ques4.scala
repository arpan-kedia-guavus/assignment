package com.assignment.scala1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first, sum, udf, when}

object ques4 {

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

    def domain_url (s: String) : String = {
      if(s.split("/").length>2)
        s.split("/")(2)

      else
        ""
    }

    val splitting = udf[String,String](domain_url)

    val urlConditionCheck = col("http_url").isNull.or(col("http_url").===(""))

    //Ques-4
    val content_typeConditionCheck = col("http_content_type").isNull.or(col("http_content_type").===(""))

    val df = sparkSession.table("arpan_test.partitioned_source_table")


    var temp1 = df.withColumn("http_content",when(content_typeConditionCheck,"").otherwise(splitting_content(col("http_content_type"))))
    var temp2 = temp1.withColumn("domain_url",when(urlConditionCheck,"").otherwise(splitting(col("http_url"))))

    var table1 = temp2.groupBy("domain_url","http_content","hour","minute").agg((sum("transaction_uplink_bytes") + sum("transaction_downlink_bytes")).alias("Tonnage"),first(col("sn_start_time")).alias("sn_start_time")).where(col("domain_url").isNotNull).where(col("domain_url").notEqual(""))
    var table2 = temp2.groupBy("domain_url","hour","minute").agg((sum("transaction_uplink_bytes") + sum("transaction_downlink_bytes")).alias("Tonnage")).where(col("domain_url").isNotNull).where(col("domain_url").notEqual(""))

    var finalTable = table1.join(table2,table1.col("domain_url") === table2.col("domain_url") and(table1.col("hour") === table2.col("hour")) and(table1.col("minute") === table2.col("minute")),"left").withColumn("percentage_of_content",(table1.col("Tonnage")/table2.col("Tonnage")*100)).alias("percentage_of_content").orderBy(table1.col("domain_url"),table1.col("hour"),table1.col("minute"))
    finalTable.select(table1.col("domain_url"),table1.col("http_content"),table1.col("sn_start_time"),table1.col("hour"),table1.col("minute"),finalTable.col("percentage_of_content"),table1.col("Tonnage")).orderBy(table1.col("hour"),table1.col("minute"),col("domain_url")).write.mode("overwrite").partitionBy("hour","minute").format("orc").saveAsTable("arpan_test.ques_4")


  }
}
