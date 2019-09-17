package com.assignment.scala1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, first, sum, udf, when}

object ques3 {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession
      .builder()
      //      .master("local[*]")
      .appName("Arpan_Assinment")
      .config("hive.metastore.uris", "thrift://jiouapp001-mst-01.gvs.ggn:9083")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()


    def domain_url (s: String) : String = {
      if(s.split("/").length>2)
        s.split("/")(2)

      else
        ""
    }

    val splitting = udf[String,String](domain_url)

    val df = sparkSession.table("arpan_test.partitioned_source_table")

    val urlConditionCheck = col("http_url").isNull.or(col("http_url").===(""))
    var ques3 = df.withColumn("domain_url",when(urlConditionCheck,"").otherwise(splitting(col("http_url"))))

    var compute = ques3.groupBy("domain_url","hour","minute").agg((sum("transaction_uplink_bytes") + sum("transaction_downlink_bytes")).alias("Tonnage"),count("domain_url").alias("no_of_hits"),first(col("sn_start_time")).alias("sn_start_time")).orderBy(col("hour"),col("minute")).where(col("domain_url").isNotNull)
    compute.select(col("domain_url"),col("sn_start_time"),col("hour"),col("minute"),col("no_of_hits"),col("Tonnage")).write.mode("overwrite").partitionBy("hour","minute").format("orc").saveAsTable("arpan_test.count_of_hits_per_domain")


  }
}
