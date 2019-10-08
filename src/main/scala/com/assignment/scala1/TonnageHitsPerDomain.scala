package com.assignment.scala1

import com.assignment.scala1.Utils.{domain_url, getSparkSession, readTable}
import org.apache.spark.sql.functions.{col,udf}
import org.slf4j.LoggerFactory

object TonnageHitsPerDomain {

  private val logger = LoggerFactory.getLogger(getClass)
  private val DBName = Constants.dbName
  private val outputTableName = "TonnageHitsPerDomain"
  private val sourcetableName = Constants.sourceTableName
  private val groupByCols = Seq(Constants.edrdomainColName,"hour","minute")
  private val aggCols = Seq(Constants.tonnageColName)
  private val partitionCols = Seq("hour","minute")

  val split_url = udf[String,String](domain_url)



  def main(args: Array[String]): Unit = {
    try {
      val spark = getSparkSession
      val df = readTable(spark, DBName, sourcetableName)

      val dfWithNotNullUrl = Utils.notNullDF(df, Constants.edrurlColName)
      val dfWithDomain = dfWithNotNullUrl.withColumn(Constants.edrdomainColName, split_url(col(Constants.edrurlColName)))
      val dfWithTonnageCol = dfWithDomain.withColumn(Constants.tonnageColName, col(Constants.edrdownloadColName) + col(Constants.edruploadColName))

      val aggregateDf = Utils.aggregate(dfWithTonnageCol, groupByCols, aggCols, true)
      Utils.writeTable(aggregateDf, partitionCols, Constants.outputFormat, DBName, outputTableName)

    }
    catch {
      case e: Exception =>
        logger.error("Exception in TonnageHitsPerDomainJob ", e)
        throw e
    }


  }

}
