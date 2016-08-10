package com.mozilla.telemetry.views

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("Cross Sectional Example").getOrCreate()

import spark.implicits._

case class longitudinal (
    client_id: String
  , geo_country: Seq[String]
  , session_length: Seq[Long]
)

def weightedMode[T <: Comparable[T]](values: Seq[T], weights: Seq[Long]) = {
  val pairs = values zip weights
  val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
  agg.maxBy(_._2)._1
}

def modalCountry(row: longitudinal) = {
  weightedMode(row.geo_country, row.session_length)
} 

def getData() = {
  val ll = spark.read.load("/home/harterrt/data/l10l_20160725_single_shard.parquet")

  val ds = ll.as[longitudinal]
  val row = ds.take(1)
  val elem = row(0)
  
  (ll, ds, row, elem)
}
