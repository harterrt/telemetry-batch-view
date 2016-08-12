package com.mozilla.telemetry.views


import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

case class longitudinal (
    client_id: String
  , geo_country: Seq[String]
  , session_length: Seq[Long]
)

case class crossSectional (
    client_id: String
  , modal_country: String
)

object CrossSectionalView {
  val sparkConf = new SparkConf().setAppName("Cross Sectional Example")
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
  implicit val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def weightedMode[T <: Comparable[T]](values: Seq[T], weights: Seq[Long]) = {
    val pairs = values zip weights
    val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
    agg.maxBy(_._2)._1
  }

  def modalCountry(row: longitudinal) = {
    weightedMode(row.geo_country, row.session_length)
  } 

  def generateCrossSectional(base: longitudinal) = {
      crossSectional(base.client_id, modalCountry(base))
  }

  def getData() = {
    val ll = sqlContext.read.load("/home/harterrt/data/l10l_20160725_single_shard.parquet")

    val ds = ll.as[longitudinal]
    val row = ds.take(1)
    val elem = row(0)
    
    (ll, ds, row, elem)
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val source = opt[String](
      "source",
      descr = "Path to l10l set source",
      required = true)
    val outputBucket = opt[String](
      "outputBucket",
      descr = "Bucket in which to save data",
      required = false,
      default=Some("telemetry-test-bucket/harter"))
    val outName = opt[String](
      "outName",
      descr = "Name for the output of this run",
      required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val source = opts.source()

    val ds = sqlContext.read.parquet(source).as[longitudinal]
    val output = ds.map(generateCrossSectional)

    val prefix = s"s3://${opts.outputBucket()}/CrossSectional/${opts.outName}"
  }
}
