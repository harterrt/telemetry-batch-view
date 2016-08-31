package com.mozilla.telemetry.views

import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.utils.S3Store

class DataSetRow() extends Product {
  // Not ideal, but a workaround until we get past the 22 field limit in case
  // classes. Create a class which implements the Product interface. We only
  // really care about the type casting. The array is used to measure arity
  // and equality
  // TODO(harter): There has to be a better way to do this
  // TODO(harter): If not, think about this choice of data structure
  private val valSeq = Array()

  def productArity() = valSeq.length
  def productElement(n: Int) = valSeq(n)
  //TODO(harter): restrict equality to a data type
  def canEqual(that: Any) = true
  override def equals(that: Any) = {
    that match {
      case that: DataSetRow => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  }
}

class Longitudinal (
    val client_id: String
  , val geo_country: Option[Seq[String]]
  , val session_length: Option[Seq[Long]]
) extends DataSetRow {
  private val valSeq = Array(client_id, geo_country, session_length)
}

case class CrossSectional (
    client_id: String
  , modal_country: Option[String]
)

object CrossSectionalView {
  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val outputBucket = opt[String](
      "outputBucket",
      descr = "Bucket in which to save data",
      required = false,
      default=Some("telemetry-test-bucket"))
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

  private[telemetry] object Aggregation {
    // Spark triggers a strange error during distributed computation if these
    // functions are in the same scope as the main function. This appears to
    // only be an issue with Spark 1.6. 
    // TODO(harter): debug this error and remove this object if possible.
    def weightedMode(values: Seq[String], weights: Seq[Long]): Option[String] = {
      if (values.size > 0 && values.size == weights.size) {
        val pairs = values zip weights
        val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
        Some(agg.maxBy(_._2)._1)
      } else {
        None
      }
    }

    def modalCountry(row: Longitudinal): Option[String] = {
      (row.geo_country, row.session_length) match {
        case (Some(gc), Some(sl)) => weightedMode(gc, sl)
        case _ => None
      }
    } 

    def generateCrossSectional(base: Longitudinal): CrossSectional = {
      val output = CrossSectional(base.client_id, modalCountry(base))
      output
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val opts = new Opts(args)

    // Read local parquet data, if supplied
    if(opts.localTable.isSupplied) {
      val localTable = opts.localTable()
      val data = hiveContext.read.parquet(localTable)
      data.registerTempTable("longitudinal")
    }

    // Calculate CrossSectional dataset
    val ds = hiveContext
      .sql("SELECT * FROM longitudinal")
      .selectExpr("client_id", "geo_country", "session_length")
      .as[Longitudinal]
    val output = ds.map(Aggregation.generateCrossSectional)

    // Save to S3
    val prefix = s"harter/CrossSectional/${opts.outName()}"
    val outputBucket = opts.outputBucket()


    require(S3Store.isPrefixEmpty(outputBucket, prefix),
      s"s3a://${outputBucket}/${prefix} already exists!")

    output.toDF().write.parquet(s"s3a://telemetry-test-bucket/harter/cross_sectional/${opts.outName()}")

    // Force the computation, debugging purposes only
    // TODO(harterrt): Remove this
    println("="*80 + "\n" + output.count + "\n" + "="*80)
  }
}
