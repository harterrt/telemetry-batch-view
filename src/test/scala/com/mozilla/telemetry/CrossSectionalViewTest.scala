package com.mozilla.telemetry

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.mozilla.telemetry.views._
import CrossSectionalView._
import org.scalatest.FlatSpec
import org.apache.spark.sql.Dataset

class CrossSectionalViewTest extends FlatSpec {
  def compareDS(actual: Dataset[CrossSectional], expected: Dataset[CrossSectional]) = {
    actual.collect.zip(expected.collect)
      .map(xx => xx._1 == xx._2)
      .reduce(_ && _)
  }

  def getExampleLongitudinal(client_id: String) = {
    val build = Build(
      application_id = "some_app_id",
      application_name = "build_app_name",
      architecture = "build_architecture",
      architecture_in_binary = "build_architecture_in_binary",
      build_id = "build_id",
      version = "build_version",
      vendor = "build_vendor" 
      platform_version = "build_platform_version" 
      xpcom_abi = "build_xpcom_abi" 
      hotfix_version = "build_hotfix_version" 
    )

    new Longitudinal(
      client_id = client_id,
      normalized_channel = "release",
      submission_date: Option(Seq("2015-01-01T00:00:00.0+00:00",
        "2015-01-02T00:00:00.0+00:00", "2015-01-03T00:00:00.0+00:00")),
      geo_country: Option(Seq("DE", "DE", "IT")),
      session_length: Option(Seq(3600, 7200, 14400))
      build = Option(Seq(build, build, build))
    )
  }

  "CrossSectional" must "be calculated correctly" in {
    val sparkConf = new SparkConf().setAppName("CrossSectionalTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val longitudinalDataset = Seq(
      new Longitudinal("a", Option(Seq("DE", "DE", "IT")), Option(Seq(2, 3, 4))),
      new Longitudinal("b", Option(Seq("EG", "EG", "DE")), Option(Seq(1, 1, 2)))
    ).toDS

    val actual = longitudinalDataset.map(xx => new CrossSectional(xx))
    val expected = Seq(
      new CrossSectional("a", Option("DE")),
      new CrossSectional("b", Option("EG"))
    ).toDS

    assert(compareDS(actual, expected))
    sc.stop()
  }

  "DataSetRows" must "distinguish between unequal rows" in {
    val l1 = new Longitudinal("id", Some(Seq("DE")), Some(Seq(1)))
    val l2 = new Longitudinal("other_id", Some(Seq("DE")), Some(Seq(1)))

    assert(l1 != l2)
  }

  it must "acknowledge equal rows" in {
    val l1 = new Longitudinal("id", Some(Seq("DE")), Some(Seq(1)))
    val l2 = new Longitudinal("id", Some(Seq("DE")), Some(Seq(1)))

    println(l1.valSeq.hashCode)
    println(l2.valSeq.hashCode)
    assert(l1 == l2)
  }
}
