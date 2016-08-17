package com.mozilla.telemetry.views


import com.mozilla.telemetry.utils._
import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext

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

  val sqlContext = new SQLContext(sc)
  sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
  import sqlContext.implicits._
     
  val hiveContext = new HiveContext(sc)
  hiveContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")

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

  def main(args: Array[String]): Unit = {
    import hiveContext.implicits._
    logger.debug("Entering main function.")
    val opts = new Opts(args)

    if(opts.localTable.isSupplied) {
      val localTable = opts.localTable()
      loadLocalData(localTable)
    }

    val ds = hiveContext.sql("SELECT * FROM longitudinal").selectExpr("client_id", "geo_country", "session_length").as[longitudinal]
    val output = ds.map(generateCrossSectional)

    val prefix = s"s3://${opts.outputBucket()}/CrossSectional/${opts.outName}"
    logger.debug("starting row count")
    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
