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
  sparkConf.set("spark.executor.memory", "4g")

  implicit val sc = new SparkContext(sparkConf)
     
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  private val logger = org.apache.log4j.Logger.getLogger("XSec")

  def weightedMode[T <: Comparable[T]](values: Seq[T], weights: Seq[Long]) = {
    val pairs = values zip weights
    val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
    agg.maxBy(_._2)._1
  }

  def loadLocalData(filename: String) = {
    val data = sqlContext.read.parquet(filename)
    data.registerTempTable("longitudinal")
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
    val localTable = opt[String](
      "localTable",
      descr = "Optional path to a local longitudinal table",
      required = false)
    val outName = opt[String](
      "outName",
      descr = "Name for the output of this run",
      required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    logger.debug("Entering main function.")
    val opts = new Opts(args)
    val localTable = opts.localTable()

    if(opts.localTable.isSupplied) {
      loadLocalData(localTable)
    }

    val ds = sqlContext.sql("SELECT * FROM longitudinal").as[longitudinal]
    val output = ds.map(xx => crossSectional(xx.client_id, xx.geo_country.head))

    val prefix = s"s3://${opts.outputBucket()}/CrossSectional/${opts.outName}"
    logger.debug("starting row count")
    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
