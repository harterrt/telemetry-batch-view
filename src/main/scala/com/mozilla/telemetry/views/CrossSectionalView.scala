package com.mozilla.telemetry.views

import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.utils.Aggregation

case class Longitudinal (
    client_id: String
  , geo_country: Option[Seq[String]]
  , session_length: Option[Seq[Long]]
) {
  def sessionWeightedMode(values: Option[Seq[String]]) = {
    (values, this.session_length) match {
      case (Some(gc), Some(sl)) => Some(Aggregation.weightedMode(gc, sl))
      case _ => None
    }
  }
}

case class CrossSectional (
    client_id: String
  , modal_country: Option[String]
) {
  def this(base: Longitudinal) = {
    this(
      client_id = base.client_id,
      modal_country = base.sessionWeightedMode(base.geo_country)
    )
  }
}

object CrossSectionalView {
  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val outputBucket = opt[String](
      "outputBucket",
      descr = "Bucket in which to save data",
      required = false,
      default=Some("telemetry-test-bucket/harter"))
    val localTable = opt[String](
      "localTable",
      descr = "Optional path to a local Parquet file with longitudinal data",
      required = false)
    val outName = opt[String](
      "outName",
      descr = "Name for the output of this run",
      required = true)
    verify()
  }

  def main(args: Array[String]): Unit = {
    // Setup spark contexts
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    // Parse command line options
    val opts = new Opts(args)

    if(opts.localTable.isSupplied) {
      val localTable = opts.localTable()
      val data = sqlContext.read.parquet(localTable)
      data.registerTempTable("longitudinal")
    }

    // Generate and save the view
    val ds = hiveContext
      .sql("SELECT * FROM longitudinal")
      .selectExpr("client_id", "geo_country", "session_length")
      .as[Longitudinal]
    val output = ds.map(xx => new CrossSectional(xx))

    val prefix = s"s3://${opts.outputBucket()}/CrossSectional/${opts.outName}"
    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
