package com.assignment.scala1

import com.assignment.scala1.Utils.{domain_url, getSparkSession, readTable}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object TonnagePerDomainPerContent {

  private val logger = LoggerFactory.getLogger(getClass)
  private val DBName = Constants.dbName
  private val outputTableName = "TonnageDomainPerContent"
  private val sourcetableName = Constants.sourceTableName
  private val groupByCols = Seq(Constants.edrdomainColName,"hour","minute")
  private val aggCols = Seq(Constants.tonnageColName)
  private val JoinSeq = Seq(Constants.edrdomainColName,"hour","minute")
  private val percentCol = "percentage_of_content"
  private val partitionCols = Seq("hour","minute")


  val split_url = udf[String,String](domain_url)

  def main(args: Array[String]): Unit = {

    try{
      val spark = getSparkSession
      val df = readTable(spark, DBName, sourcetableName)

      val dfWithNotNullContent = Utils.notNullDF(df,Constants.edrcontentTypeColName)
      val dfWithContentCol = dfWithNotNullContent.withColumn(Constants.edrcontentTypeColName,Utils.split_content(col(Constants.edrcontentTypeColName)))

      val dfWithNotNullUrl = Utils.notNullDF(dfWithContentCol,Constants.edrurlColName)
      val dfWithDomain = dfWithNotNullUrl.withColumn(Constants.edrdomainColName,split_url(col(Constants.edrurlColName)))
      val dfWithTonnageCol = dfWithDomain.withColumn(Constants.tonnageColName, col(Constants.edrdownloadColName) + col(Constants.edruploadColName))


      val contentTypeDF = readTable(spark,DBName,Constants.contentTypeTableName)
      val joinedDF = dfWithTonnageCol.join(contentTypeDF,Seq(Constants.edrcontentTypeColName)).persist(StorageLevel.MEMORY_AND_DISK)

      val aggregateDFWithContent = Utils.aggregate(joinedDF,groupByCols ++ Seq(Constants.edrcontentTypeColName), aggCols,false)
      val aggregateDFWithoutContent = Utils.aggregate(joinedDF,groupByCols,aggCols,false)

      val outputDF = aggregateDFWithContent.join(aggregateDFWithoutContent,JoinSeq)
      val outputDFWithPercentageCol = outputDF.withColumn(percentCol,(aggregateDFWithContent.col("total_" + Constants.tonnageColName)/aggregateDFWithoutContent.col("total_" + Constants.tonnageColName))*100).drop(aggregateDFWithoutContent.col("total_" + Constants.tonnageColName))
      Utils.writeTable(outputDFWithPercentageCol,partitionCols,Constants.outputFormat,DBName,outputTableName)

    }

    catch {
      case e: Exception =>
        logger.error("Exception in TonnagePerDomainPerContentJob ", e)
        throw e
    }
  }

}
