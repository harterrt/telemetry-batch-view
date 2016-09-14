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
      case that: DataSetRow => this.canEqual(that) && this.valSeq.deep == that.valSeq.deep
      case _ => false
    }
  }
  
  def compare(that: Any, epsilon: Double = 1E-7) = {
    //This is like equal, but with fuzzy match for doubles.
    //It's better not to override equality because this functions will not
    //necessarily preserve transitivity.

    def compareElement(pair: (Any, Any)) = {
      pair match {
        case (Some(first: Double), Some(second: Double)) => Math.abs(first - second) < epsilon
        case (first: Any, second: Any) => first == second
        case _ => false
      }
    }

    that match {
      case that: DataSetRow => {this.canEqual(that) &&
        this.valSeq.zip(that.valSeq).foldLeft(true)((acc, pair) => acc && compareElement(pair))
      }
      case _ => false
    }
  }
}

object Longitudinal {
}

class Longitudinal (
    val client_id: String
  , val normalized_channel: String
  , val submission_date: Seq[String]
  , val geo_country: Seq[String]
  , val session_length: Seq[Long]
  , val is_default_browser: Seq[Option[Boolean]]
  , val default_search_engine: Seq[Option[String]]
  , val locale: Seq[Option[String]]
  , val architecture: Seq[Option[String]]
) extends DataSetRow {
  override val valSeq = Array[Any](client_id, geo_country, session_length)

  def weightedMode[A](values: Seq[A]): A = {
    Aggregation.weightedMode(values, this.session_length)
  }

  def distinctConfigs[A](acc: (Longitudinal) => Seq[A]) = {
    acc(this).distinct.length
  }

  def parsedSubmissionDate() = {
    val date_parser =  new java.text.SimpleDateFormat("yyyy-MM-dd")
    this.submission_date.map(date_parser.parse(_))
  }

  // TODO(harterrt): acc is a bad variable name for accessor
  private def filterByDOW[A](acc: Longitudinal => Seq[A], dow: Int): Seq[A] = {
    (acc(this) zip this.parsedSubmissionDate).filter(_._2.getDay == dow).map(_._1)
  }

  def activeHoursByDOW(dow: Int) = {
    this.filterByDOW(_.session_length, dow).sum/3600.0
  }

  def getAll[Group, Field]
    (chooseGroup: (Longitudinal) => Seq[Group])
    (chooseField: (Group) => Field): Seq[Field] = {
    // TODO(harterrt): Fix this comment structure
    // Most columns are Seq of objects, when we really want an object
    // containing sequences for each field. This function takes an accessor
    // function for the Seq and Field and generates an Seq of all field
    // values.
    // TODO(harterrt): Should this be implemented as a subgroups's property?
    chooseGroup(this).map(chooseField)
  }
}

class CrossSectional (
    val client_id: String
  , val normalized_channel: String
  , val active_hours_total: Double
  , val active_hours_sun: Double
  , val active_hours_mon: Double
  , val active_hours_tue: Double
  , val active_hours_wed: Double
  , val active_hours_thu: Double
  , val active_hours_fri: Double
  , val active_hours_sat: Double
  , val geo_Mode: String
  //, val geo_Cfgs: Long // TODO(harterrt) Make optional, for some reason.
  , val architecture_Mode: Option[String]
  , val ffLocale_Mode: Option[String]
) extends DataSetRow {
  override val valSeq = Array[Any](client_id, normalized_channel,
    active_hours_total, active_hours_sun, active_hours_mon, active_hours_tue,
    active_hours_wed, active_hours_thu, active_hours_fri, active_hours_sat,
    geo_Mode, architecture_Mode, ffLocale_Mode)

  def this(base: Longitudinal) = {
    this(
      client_id = base.client_id,
      normalized_channel = base.normalized_channel,
      active_hours_total = base.session_length.sum,
      active_hours_sun = base.activeHoursByDOW(0),
      active_hours_mon = base.activeHoursByDOW(1),
      active_hours_tue = base.activeHoursByDOW(2),
      active_hours_wed = base.activeHoursByDOW(3),
      active_hours_thu = base.activeHoursByDOW(4),
      active_hours_fri = base.activeHoursByDOW(5),
      active_hours_sat = base.activeHoursByDOW(6),
      geo_Mode = base.weightedMode(base.geo_country),
      //geo_Cfgs = base.distinctConfigs(_.geo_country),
      architecture_Mode = base.weightedMode(base.architecture),
      ffLocale_Mode = base.weightedMode(base.locale)
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
      val localTable = opts.localTable()
      val data = hiveContext.read.parquet(localTable)
      data.registerTempTable("longitudinal")
    }

    // Generate and save the view
    val ds = hiveContext
      .sql("SELECT * FROM longitudinal")
      .selectExpr(
        "client_id", "normalized_channel", "submission_date", "geo_country",
        "session_length", "settings.locale", "settings.is_default_browser",
        "settings.default_search_engine", "build.architecture"
      )
      .na.drop()
      .as[Longitudinal]
    val output = ds.map(new CrossSectional(_))

    // Save to S3
    val prefix = s"cross_sectional/${opts.outName()}"
    val outputBucket = opts.outputBucket()
    val path = s"s3a://${outputBucket}/${prefix}"


    require(S3Store.isPrefixEmpty(outputBucket, prefix),
      s"${path} already exists!")

    output.toDF().write.parquet(path)

    val ex = output.take(2)
    println("="*80 + "\n" + ex + "\n" + "="*80)
  }
}
