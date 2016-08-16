package com.mozilla.telemetry.views


import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

case class longitudinal (
    client_id: String
  , geo_country: Option[Seq[String]]
  , session_length: Option[Seq[Long]]
)

case class crossSectional (
    client_id: String
  , modal_country: Option[String]
)

object CrossSectionalView {
  val sparkConf = new SparkConf().setAppName("Cross Sectional Example")
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
  implicit val sc = new SparkContext(sparkConf)
     
  val sqlContext = new HiveContext(sc)
  import sqlContext.implicits._

  private val logger = org.apache.log4j.Logger.getLogger("XSec")

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

  def loadLocalData(filename: String) = {
    val data = sqlContext.read.parquet(filename)
    data.registerTempTable("longitudinal")
  }

  def weightedMode(values: Seq[String], weights: Seq[Long]): Option[String] = {
    if (values.size > 0 && values.size == weights.size) {
      val pairs = values zip weights
      val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
      Some(agg.maxBy(_._2)._1)
    } else {
      Option(null)
    }
  }

  def modalCountry(row: longitudinal): Option[String] = {
    (row.geo_country, row.session_length) match {
      case (Some(gc), Some(sl)) => weightedMode(gc, sl)
      case _ => Option(null)
    }
  } 

  def generateCrossSectional(base: longitudinal): crossSectional = {
    val output = crossSectional(base.client_id, modalCountry(base))
    output
  }

  def main(args: Array[String]): Unit = {
    logger.debug("Entering main function.")
    val opts = new Opts(args)

    if(opts.localTable.isSupplied) {
      val localTable = opts.localTable()
      loadLocalData(localTable)
    }

    val ds = sqlContext.sql("SELECT * FROM longitudinal").as[longitudinal]
    val output = ds.map(generateCrossSectional)

    val prefix = s"s3://${opts.outputBucket()}/CrossSectional/${opts.outName}"
    logger.debug("starting row count")
    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
