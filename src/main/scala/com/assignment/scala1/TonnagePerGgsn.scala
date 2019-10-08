package com.assignment.scala1

import com.assignment.scala1.Utils.{getSparkSession, readTable}
import org.apache.spark.sql.functions.{col}
import org.slf4j.LoggerFactory

object TonnagePerGgsn {

  private val logger = LoggerFactory.getLogger(getClass)
  private val DBName = Constants.dbName
  private val sourcetableName = Constants.sourceTableName
  private val joinSeq = Seq(Constants.ggsnIpCol)
  private val uploadColName = Constants.edruploadColName
  private val downloadcolName = Constants.edrdownloadColName
  private val tonnageColName = Constants.tonnageColName
  private val groupByCols = Seq(Constants.ggsnNameCol)
  private val aggCols = Seq(tonnageColName)
  private val outputTableName = "tonnage_per_ggsn"


  def main(args: Array[String]): Unit = {
    try{
      val spark = getSparkSession
      val df = readTable(spark, DBName, sourcetableName)
      val dfWithTonnageCol = df.withColumn(tonnageColName,col(uploadColName) + col(downloadcolName))


      spark.sql("CREATE EXTERNAL TABLE if not exists " + DBName + "." + Constants.ggsnTableName +" (`ggsn_name` string,`ggsn_ip` ARRAY<STRUCT<value: STRING>>)ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe' WITH SERDEPROPERTIES (\"column.xpath.ggsn_name\"=\"ggsn/@name\",\"column.xpath.ggsn_ip\"=\"ggsn/rule/condition\") STORED AS INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION '" + Constants.ggsnTableLoc + "' TBLPROPERTIES (  \"xmlinput.start\"=\"<ggsn\",\"xmlinput.end\"=\"</ggsn>\")")
      spark.sql("create table if not exists " + DBName + "." + Constants.ggsnFormattedTable + " as\nSELECT ggsn_name as ggsn_name, prod_and_ts.value as ggsn_ip\nFROM  ggsn_table  LATERAL VIEW explode(ggsn_ip) ggsn_table as prod_and_ts")
      val ggsnTable = readTable(spark,DBName,Constants.ggsnFormattedTable)

      val joinedDF = dfWithTonnageCol.join(ggsnTable,joinSeq)
      val dfWithNotNullGgsn = Utils.notNullDF(joinedDF,Constants.ggsnNameCol)
      val aggregateDF = Utils.aggregate(dfWithNotNullGgsn,groupByCols,aggCols,false)
      aggregateDF.write.mode("overwrite").format("orc").saveAsTable(DBName + "." + outputTableName)


    }
    catch {
      case e: Exception =>
        logger.error("Exception in TonnagePerGGSNJob ", e)
        throw e
    }

  }

}
