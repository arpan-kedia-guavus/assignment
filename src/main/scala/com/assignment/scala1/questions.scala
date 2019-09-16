package com.assignment.scala1

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object questions {

  def main(args: Array[String]): Unit ={


    val sparkSession = SparkSession
      .builder()
      //      .master("local[*]")
      .appName("Arpan_Assinment")
      .config("hive.metastore.uris","thrift://jiouapp001-mst-01.gvs.ggn:9083")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .config("spark.hadoop.dfs.replication", 2)
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

    //--------------------------------------------------------------------------------------------------------------------------------------

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

   // -------------------------------------------------------------------------------------------------------------------------------------------------------------

    //Ques-3

    def domain_url (s: String) : String = {
      if(s.split("/").length>2)
      s.split("/")(2)

      else
        ""
    }

    val splitting = udf[String,String](domain_url)

    val urlConditionCheck = col("http_url").isNull.or(col("http_url").===(""))
    var ques3 = df.withColumn("domain_url",when(urlConditionCheck,"").otherwise(splitting(col("http_url"))))

    var compute = ques3.groupBy("domain_url","hour","minute").agg((sum("transaction_uplink_bytes") + sum("transaction_downlink_bytes")).alias("Tonnage"),count("domain_url").alias("no_of_hits"),first(col("sn_start_time")).alias("sn_start_time")).orderBy(col("hour"),col("minute")).where(col("domain_url").isNotNull)
    compute.select(col("domain_url"),col("sn_start_time"),col("hour"),col("minute"),col("no_of_hits"),col("Tonnage")).write.mode("overwrite").partitionBy("hour","minute").format("orc").saveAsTable("arpan_test.count_of_hits_per_domain")

    // -----------------------------------------------------------------------------------------------------------------------------------------------------------------

    //Ques--4

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

    //Ques-4
    val content_typeConditionCheck = col("http_content_type").isNull.or(col("http_content_type").===(""))

    var temp1 = df.withColumn("http_content",when(content_typeConditionCheck,"").otherwise(splitting_content(col("http_content_type"))))
    var temp2 = temp1.withColumn("domain_url",when(urlConditionCheck,"").otherwise(splitting(col("http_url"))))

    var table1 = temp2.groupBy("domain_url","http_content","hour","minute").agg((sum("transaction_uplink_bytes") + sum("transaction_downlink_bytes")).alias("Tonnage"),first(col("sn_start_time")).alias("sn_start_time")).where(col("domain_url").isNotNull).where(col("domain_url").notEqual(""))
    var table2 = temp2.groupBy("domain_url","hour","minute").agg((sum("transaction_uplink_bytes") + sum("transaction_downlink_bytes")).alias("Tonnage")).where(col("domain_url").isNotNull).where(col("domain_url").notEqual(""))

    var finalTable = table1.join(table2,table1.col("domain_url") === table2.col("domain_url") and(table1.col("hour") === table2.col("hour")) and(table1.col("minute") === table2.col("minute")),"left").withColumn("percentage_of_content",(table1.col("Tonnage")/table2.col("Tonnage")*100)).alias("percentage_of_content").orderBy(table1.col("domain_url"),table1.col("hour"),table1.col("minute"))
    finalTable.select(table1.col("domain_url"),table1.col("http_content"),table1.col("sn_start_time"),table1.col("hour"),table1.col("minute"),finalTable.col("percentage_of_content"),table1.col("Tonnage")).orderBy(table1.col("hour"),table1.col("minute"),col("domain_url")).write.mode("overwrite").partitionBy("hour","minute").format("orc").saveAsTable("arpan_test.ques_4")

    //-----------------------------------------------------------------------------------------------------------------------------------------

   // Ques-2
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



    //------------------------------------------------------------------------------------------------------------------------------------------------------------
    //Ques--5
    sparkSession.sql("CREATE EXTERNAL TABLE if not exists arpan_test.ggsn_table (\n`ggsn_name` string,\n`ggsn_ip` ARRAY<STRUCT<value: STRING>>\n)\n\nROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'\nWITH SERDEPROPERTIES (\n\n\"column.xpath.ggsn_name\"=\"ggsn/@name\",\n\"column.xpath.ggsn_ip\"=\"ggsn/rule/condition\"\n)\n\nSTORED AS INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'\n    OUTPUTFORMAT \n'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' \n    LOCATION '/data/arpan/ggsn_table'\n    TBLPROPERTIES (\n    \"xmlinput.start\"=\"<ggsn\",\"xmlinput.end\"=\"</ggsn>\"\n)")
    sparkSession.sql("create table if not exists arpan_test.ggsn_table_formatted as\nSELECT\n   ggsn_name as ggsn_name,\n   prod_and_ts.value as ggsn_ip\nFROM \n   ggsn_table \n   LATERAL VIEW explode(ggsn_ip) ggsn_table as prod_and_ts")
    val ggsn_table_formatted = sparkSession.table("arpan_test.ggsn_table_formatted")
    val ggsn_table_join = df.join(ggsn_table_formatted,df.col("ggsn_ip") === ggsn_table_formatted.col("ggsn_ip"),"left")
    val ques5 = ggsn_table_join.groupBy("ggsn_name").agg((sum("transaction_uplink_bytes") + sum("transaction_downlink_bytes")).alias("Tonnage")).where(col("ggsn_name").isNotNull)
    ques5.write.mode("overwrite").format("orc").saveAsTable("arpan_test.tonnage_per_ggsn_name")

  }





}
