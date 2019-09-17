package com.assignment.scala1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object ques5 {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession
      .builder()
      //      .master("local[*]")
      .appName("Arpan_Assinment")
      .config("hive.metastore.uris", "thrift://jiouapp001-mst-01.gvs.ggn:9083")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val df = sparkSession.table("arpan_test.partitioned_source_table")

    sparkSession.sql("CREATE EXTERNAL TABLE if not exists arpan_test.ggsn_table (\n`ggsn_name` string,\n`ggsn_ip` ARRAY<STRUCT<value: STRING>>\n)\n\nROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'\nWITH SERDEPROPERTIES (\n\n\"column.xpath.ggsn_name\"=\"ggsn/@name\",\n\"column.xpath.ggsn_ip\"=\"ggsn/rule/condition\"\n)\n\nSTORED AS INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'\n    OUTPUTFORMAT \n'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' \n    LOCATION '/data/arpan/ggsn_table'\n    TBLPROPERTIES (\n    \"xmlinput.start\"=\"<ggsn\",\"xmlinput.end\"=\"</ggsn>\"\n)")
    sparkSession.sql("create table if not exists arpan_test.ggsn_table_formatted as\nSELECT\n   ggsn_name as ggsn_name,\n   prod_and_ts.value as ggsn_ip\nFROM \n   ggsn_table \n   LATERAL VIEW explode(ggsn_ip) ggsn_table as prod_and_ts")
    val ggsn_table_formatted = sparkSession.table("arpan_test.ggsn_table_formatted")
    val ggsn_table_join = df.join(ggsn_table_formatted,df.col("ggsn_ip") === ggsn_table_formatted.col("ggsn_ip"),"left")
    val ques5 = ggsn_table_join.groupBy("ggsn_name").agg((sum("transaction_uplink_bytes") + sum("transaction_downlink_bytes")).alias("Tonnage")).where(col("ggsn_name").isNotNull)
    ques5.write.mode("overwrite").format("orc").saveAsTable("arpan_test.tonnage_per_ggsn_name")


  }
}
