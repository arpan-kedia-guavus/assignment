package com.assignment.scala1

import com.assignment.scala1.Utils._
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object Top10Subscriber {

  private val logger = LoggerFactory.getLogger(getClass)
  private val DBName = Constants.dbName
  private val sourcetableName = Constants.sourceTableName
  private val outputTableName = "Top10_Subscriber"
  private val subscriberColName = Constants.edrSubscriberColName
  private val uploadColName = Constants.edruploadColName
  private val downloadcolName = Constants.edrdownloadColName
  private val tonnageColName = Constants.tonnageColName
  private val aggCols = Seq(uploadColName, downloadcolName, tonnageColName)
  private val groupByCols = Seq(subscriberColName,"hour")
  private val partitionCols = Seq("hour")


  def main(args: Array[String]): Unit = {
    try {
      val spark : SparkSession = getSparkSession
      val df = readTable(spark,DBName,sourcetableName)

      val dfWithTonnageCol = df.withColumn(tonnageColName,col(uploadColName) + col(downloadcolName))
      val aggregateDf = Utils.aggregate(dfWithTonnageCol,groupByCols,aggCols,false)

      val downloadDF = dfWithPartition(aggregateDf,"total_" + downloadcolName)
      val uploadDF = dfWithPartition(aggregateDf,"total_" +uploadColName)
      val tonnageDF = dfWithPartition(aggregateDf,"total_" +tonnageColName)

      val outputDF = downloadDF.join(uploadDF, "hour").join(tonnageDF, "hour")
      Utils.writeTable(outputDF,partitionCols,Constants.outputFormat,DBName,outputTableName)

    }

    catch {
      case e: Exception =>
        logger.error("Exception in Top10SubscriberJob ", e)
        throw e
    }
  }


  def dfWithPartition(aggregatedDF: DataFrame, colName: String): DataFrame =
  {
    val window = org.apache.spark.sql.expressions.Window.partitionBy("hour").orderBy(desc(colName))
    val dfWithRowNumCol =  aggregatedDF.withColumn("row_num", rank() over window).filter(col("row_num") <= 10)
    val dfWithSubsList = dfWithRowNumCol.select(col("hour"), col(Constants.edrSubscriberColName)).groupBy(col("hour")).agg(collect_list(col(Constants.edrSubscriberColName)).as("Top_Subscribers_basis_of_" + colName))

    dfWithSubsList
  }

}
