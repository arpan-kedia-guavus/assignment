package com.assignment.scala1

import com.assignment.scala1.Utils._
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

object Top5ContentType {

  private val logger = LoggerFactory.getLogger(getClass)
  private val DBName = Constants.dbName
  private val outputTableName = "Top5_Content_Type"
  private val sourcetableName = Constants.sourceTableName
  private val groupByCols = Seq(Constants.edrSubscriberColName,Constants.httpContentColName,"hour")
  private val aggCols = Seq(Constants.edruploadColName,Constants.edrdownloadColName)
  private val JoinSeq = Seq(Constants.edrSubscriberColName,"hour")
  private val partitionCols = Seq("hour")

  def main(args: Array[String]): Unit = {

    try{
      val spark = getSparkSession
      val df = readTable(spark,DBName,sourcetableName)
      val ContentTypeDF = getContentTypeDF(df,Constants.edrcontentTypeColName)

      val dfWithNotNullContentType = notNullDF(df,Constants.edrcontentTypeColName)
      val dfWithContentTypeCol = dfWithNotNullContentType.withColumn(Constants.edrcontentTypeColName,split_content(col(Constants.edrcontentTypeColName)))

      val joinedTable = dfWithContentTypeCol.join(ContentTypeDF,Seq(Constants.edrcontentTypeColName))
      val aggregateDf = Utils.aggregate(joinedTable,groupByCols,aggCols,true).persist(StorageLevel.MEMORY_AND_DISK)

      val downloadDF = dfWithPartition(aggregateDf,"total_" + Constants.edrdownloadColName)
      val uploadDF = dfWithPartition(aggregateDf,"total_" +Constants.edruploadColName)
      val hitsDF = dfWithPartition(aggregateDf,Constants.HitsColName)

      val outputDF = downloadDF.join(uploadDF, JoinSeq).join(hitsDF, JoinSeq)
      Utils.writeTable(outputDF,partitionCols,Constants.outputFormat,DBName,outputTableName)

    }
    catch {
      case e: Exception =>
        logger.error("Exception in Top5ContentTypeJob ", e)
        throw e
    }
  }

  def dfWithPartition(aggregatedDF: DataFrame, colName: String): DataFrame =
  {
    val window = org.apache.spark.sql.expressions.Window.partitionBy(col(Constants.edrSubscriberColName),col("hour")).orderBy(desc(colName))
    val dfWithRowNumCol =  aggregatedDF.withColumn("row_num", rank() over window).filter(col("row_num") <= 5)
    val dfWithSubsList = dfWithRowNumCol.select(col(Constants.edrSubscriberColName),col("hour"),col(Constants.httpContentColName)).groupBy(col("hour"),col(Constants.edrSubscriberColName)).agg(collect_list(col(Constants.httpContentColName)).as("Top_httpContent_basis_of_" + colName))

    dfWithSubsList
  }



}
