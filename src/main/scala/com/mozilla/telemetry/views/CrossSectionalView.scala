package com.mozilla.telemetry.views

import org.rogach.scallop._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.utils.S3Store
import com.mozilla.telemetry.utils.Aggregation

abstract class DataSetRow() extends Product {
  // This class is a work around the 22 field limit in case classes. The 22
  // field limit is removed in scala 2.11, but until we upgrade our spark
  // clusters, we have to work with 2.10.
  // Spark only includes encoders for primitive types and objects implementing
  // the Product interface.
  val valSeq: Array[Any]

  def productArity() = valSeq.length
  def productElement(n: Int) = valSeq(n)
  //TODO(harter): restrict equality to a data type
  def canEqual(that: Any) = true
  override def equals(that: Any) = {
    that match {
      case that: DataSetRow => that.canEqual(this) && this.valSeq.deep == that.valSeq.deep
      case _ => false
    }
  }
}

case class DefaultSearchEngineData (
    val name: String
  , val load_path: String
  , val submission_url: String
)

case class Update (
    val channel: String
  , val enabled: Boolean
  , val auto_download: Boolean
)

case class Settings (
   val addon_compatibility_check_enabled: Boolean
 , val blocklist_enabled: Boolean
 , val is_default_browser: Boolean
 , val default_search_engine: String
 , val default_search_engine_data: DefaultSearchEngineData
 , val search_cohort: String
 , val e10s_enabled: Boolean
 , val telemetry_enabled: Boolean
 , val locale: String
 , val update: Update
 , var user_prefs: scala.collection.Map[String, String]
)

case class Build (
    val application_id: String
  , val application_name: String
  , val architecture: String
  , val architectures_in_binary: String
  , val build_id: String
  , val version: String
  , val vendor: String
  , val platform_version: String
  , val xpcom_abi: String
  , val hotfix_version: String
)

class Longitudinal (
    val client_id: String
  , val normalized_channel: String
  , val geo_country: Option[Seq[String]]
  , val session_length: Option[Seq[Long]]
  , val build: Option[Seq[Build]]
  , val settings: Option[Seq[Settings]]
) extends DataSetRow {
  override val valSeq = Array[Any](client_id, geo_country, session_length, build)

  def weightedMode(values: Option[Seq[String]]) = {
    (values, this.session_length) match {
      case (Some(gc), Some(sl)) => Some(Aggregation.weightedMode(gc, sl))
      case _ => None
    }
  }

  def getAll[Group, Field]
    (chooseGroup: (Longitudinal) => Option[Seq[Group]])
    (chooseField: (Group) => Field): Option[Seq[Field]] = {
    // TODO(harterrt): Fix this comment structure
    // Most columns are Seq of objects, when we really want an object
    // containing sequences for each field. This function takes an accessor
    // function for the Seq and Field and generates an Seq of all field
    // values.
    // TODO(harterrt): Should this be implemented as a subgroups's property?
    chooseGroup(this) match {
      case Some(yy) => Some(yy.map(chooseField))
      case _ => None
    }
  }
}

class CrossSectional (
    val client_id: String
  , val normalized_channel: String
  , val geo_Mode: Option[String]
  , val architecture_Mode: Option[String]
  , val ffLocale_Mode: Option[String]
) extends DataSetRow {
  override val valSeq = Array[Any](client_id, geo_Mode, architecture_Mode)

  def this(base: Longitudinal) = {
    this(
      client_id = base.client_id,
      normalized_channel = base.normalized_channel,
      geo_Mode = base.weightedMode(base.geo_country),
      architecture_Mode = base.weightedMode(base.getAll(_.build)(_.architecture)),
      ffLocale_Mode = base.weightedMode(base.getAll(_.settings)(_.locale))
    )
  }
}

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

  def main(args: Array[String]): Unit = {
    // Setup spark contexts
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    // Parse command line options
    val opts = new Opts(args)

    // Read local parquet data, if supplied
    if(opts.localTable.isSupplied) {
      println("HERE"*20)
      val localTable = opts.localTable()
      val data = hiveContext.read.parquet(localTable)
      data.registerTempTable("longitudinal")
      println(hiveContext.tableNames())
    }

    // Generate and save the view
    val ds = hiveContext
      .sql("SELECT * FROM longitudinal")
      .selectExpr(
        "client_id", "normalized_channel", "geo_country", "session_length",
        "build", "settings"
      )
      .as[Longitudinal]
    val output = ds.map(new CrossSectional(_))

    // Save to S3
    val prefix = s"cross_sectional/${opts.outName()}"
    val outputBucket = opts.outputBucket()
    val path = s"s3://${outputBucket}/${prefix}"


    require(S3Store.isPrefixEmpty(outputBucket, prefix),
      s"${path} already exists!")

    output.toDF().write.parquet(path)

    val ex = output.take(2)
    println("="*80 + "\n" + ex + "\n" + "="*80)
  }
}
