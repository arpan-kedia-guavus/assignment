package com.assignment.scala1

object Constants {

  val APP_NAME = "appName"
  val HIVE_METASTORE_URI = "thrift://jiouapp001-mst-01.gvs.ggn:9083"
  val dbName = "arpan_test"
  val sourceTableName = "partitioned_source_table"
  val contentTypeTableName = "ContentTypeTable"
  val edrSubscriberColName = "radius_user_name"
  val edrdownloadColName = "transaction_downlink_bytes"
  val edruploadColName = "transaction_uplink_bytes"
  val tonnageColName = "bytes"
  val edrurlColName = "http_url"
  val edrdomainColName = "domain_name"
  val edrcontentTypeColName = "http_content_type"
  val httpContentColName = "http_content"
  val edrreplyCodeColName = "http_reply_code"
  val HitsColName = "total_hits_count"
  val ggsnTableName = "ggsn_table"
  val ggsnFormattedTable = "ggsn_table_formatted"
  val ggsnTableLoc = "/data/arpan/ggsn_table"
  val ggsnIpCol = "ggsn_ip"
  val ggsnNameCol = "ggsn_name"
  val outputFormat = "orc"


}
