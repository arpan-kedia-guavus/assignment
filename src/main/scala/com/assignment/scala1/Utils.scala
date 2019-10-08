package com.assignment.scala1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Utils {

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

  val splitting_contentType = udf[String,String](content_type)

  val split_content = udf[String,String](getContent)

  val splitting = udf[String,String](domain_url)

  def getContent(content: String): String = {
    val splitContent = content.toLowerCase.split(";")
    splitContent(0)
  }

  def getSparkSession : SparkSession =
    SparkSession.builder().appName(Constants.APP_NAME).config("hive.metastore.uris",Constants.HIVE_METASTORE_URI).enableHiveSupport().getOrCreate()


  def readTable(sparkSession: SparkSession, dbName: String, tableName: String): DataFrame =
  {
    val tableDF = sparkSession.sql("select * from " + dbName + "." + tableName)
    tableDF
  }



  def writeTable(inputDF: DataFrame, partitionCols: Seq[String], outputFormat: String, outputTableDB: String, outputTableName: String) =
  {
    inputDF.write.mode("append").partitionBy(partitionCols: _*).format(outputFormat).saveAsTable(outputTableDB + "." + outputTableName)
  }



  def notNullDF(inputDF: DataFrame, inputColumn:String): DataFrame = {
    val df = inputDF.filter(col(inputColumn).isNotNull.or(col(inputColumn) =!= ""))
    df
  }



  def getContentTypeDF(inputDF: DataFrame, column:String): DataFrame = {

    val dfWithNotNullHttpContent = notNullDF(inputDF,column)
    val dfWithContentType =  dfWithNotNullHttpContent.select(split_content(col(Constants.edrcontentTypeColName)).alias(Constants.edrcontentTypeColName)).withColumn(Constants.httpContentColName,splitting_contentType(col(Constants.edrcontentTypeColName)))
    val dfWithDropDuplicate = dfWithContentType.dropDuplicates(Constants.edrcontentTypeColName)
    dfWithDropDuplicate.write.mode("ignore").format(Constants.outputFormat).saveAsTable(Constants.dbName + "." + Constants.contentTypeTableName)
    dfWithDropDuplicate
  }



  def aggregate(inputDF : DataFrame, groupbyCols : Seq[String], aggCols: Seq[String], calculateHits: Boolean) : DataFrame = {
    val aggSeq = aggCols.map(c => sum(col(c)).as("total_" + c))
    if(calculateHits){
      val aggSeqWithHits = aggSeq ++ Seq(count(Constants.edrreplyCodeColName).alias(Constants.HitsColName))
      val dfWithHits = inputDF.filter(col(Constants.edrreplyCodeColName).startsWith("2"))
        val aggDF = dfWithHits.groupBy(groupbyCols.map(c => col(c)): _*).agg(aggSeqWithHits.head, aggSeqWithHits.tail: _*)
      aggDF
    }
    else {
      val aggDF = inputDF.groupBy(groupbyCols.map(c => col(c)): _*).agg(aggSeq.head, aggSeq.tail: _*)
      aggDF
    }

  }



  def domain_url (s: String) : String = {
    if(s.split("/").length>2)
      s.split("/")(2)

    else
      ""
  }




}
