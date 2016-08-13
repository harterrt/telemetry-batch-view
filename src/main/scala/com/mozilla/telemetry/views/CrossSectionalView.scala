package com.mozilla.telemetry.views


import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext 

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
     
  val hiveContext = new HiveContext(sc)
  import hiveContext.implicits._

  private val logger = org.apache.log4j.Logger.getLogger("XSec")

  def weightedMode[T <: Comparable[T]](values: Seq[T], weights: Seq[Long]) = {
    val pairs = values zip weights
    val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
    agg.maxBy(_._2)._1
  }

  def modalCountry(row: longitudinal) = {
    weightedMode(row.geo_country, row.session_length)
  } 

  def generateCrossSectional(base: longitudinal) = {
    logger.debug(s"Generate xsec called with geo_country: $base.geo_country")
    val output = crossSectional(base.client_id, modalCountry(base))
    logger.debug(s"Exiting from geo_country: $base.geo_country")
    output
  }

  private class Opts(args: Array[String]) extends ScallopConf(args) {
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
    logger.debug("Entering main function.")
    val opts = new Opts(args)

    val ds = hiveContext.sql("SELECT * FROM longitudinal").as[longitudinal]
    val output = ds.map(generateCrossSectional)

    val prefix = s"s3://${opts.outputBucket()}/CrossSectional/${opts.outName}"
    logger.debug("starting row count")
    println(output.count())
  }
}
