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
      .map(x=> x._1.compare(x._2))
      .reduce(_ && _)
  }

  def getExampleLongitudinal(client_id: String) = {
    val default_search_engine = DefaultSearchEngineData (
      name = "grep",
      load_path = "test",
      submission_url = "cli"
    )

    val update = Update (
      channel = "update_channel",
      enabled = true,
      auto_download = false
    )

    val settings = Settings (
      addon_compatibility_check_enabled = true,
      blocklist_enabled = false,
      is_default_browser = false,
      default_search_engine = "grep",
      default_search_engine_data = default_search_engine,
      search_cohort = "a_cohort",
      e10s_enabled = false,
      telemetry_enabled = true,
      locale = "en_US",
      update = update,
      user_prefs = Map("browser.download.lastDir" -> "/home/johnny/Desktop")
    )

    val build = Build(
      application_id = "some_app_id",
      application_name = "build_app_name",
      architecture = "build_architecture",
      architectures_in_binary = "build_architecture_in_binary",
      build_id = "build_id",
      version = "build_version",
      vendor = "build_vendor",
      platform_version = "build_platform_version",
      xpcom_abi = "build_xpcom_abi",
      hotfix_version = "build_hotfix_version"
    )

    new Longitudinal(
      client_id = client_id,
      normalized_channel = "release",
      submission_date = Some(Seq("2016-01-01T00:00:00.0+00:00",
        "2016-01-02T00:00:00.0+00:00", "2016-01-03T00:00:00.0+00:00")),
      geo_country = Some(Seq("DE", "DE", "IT")),
      session_length = Some(Seq(3600, 7200, 14400)),
      build = Some(Seq(build, build, build)),
      settings = Some(Seq(settings, settings, settings))
    )
  }

  def getExampleCrossSectional(client_id: String) = {
    new CrossSectional(
      client_id = client_id,
      normalized_channel = "release",
      active_hours_total = Some(25200),
      active_hours_sun = Some(14400 / 3600.0),
      active_hours_mon = Some(0.0),
      active_hours_tue = Some(0.0),
      active_hours_wed = Some(0.0),
      active_hours_thu = Some(0.0),
      active_hours_fri = Some(3600/3600.0),
      active_hours_sat = Some(7200/3600.0),
      geo_Mode = Some("IT"),
      geo_Cfgs = 2,
      architecture_Mode = Some("build_architecture"),
      ffLocale_Mode = Some("en_US")
    )
  }

  "CrossSectional" must "be calculated correctly" in {
    val sparkConf = new SparkConf().setAppName("CrossSectionalTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val longitudinalDataset = Seq(
      getExampleLongitudinal("a"), getExampleLongitudinal("b")
    ).toDS

    val actual = longitudinalDataset.map(new CrossSectional(_))
    val expected = Seq(
      getExampleCrossSectional("a"),
      getExampleCrossSectional("b")
    ).toDS

    assert(compareDS(actual, expected))
    sc.stop()
  }

  "DataSetRows" must "distinguish between unequal rows" in {
    val l1 = getExampleLongitudinal("id")
    val l2 = getExampleLongitudinal("other_id")

    assert(l1 != l2)
  }

  it must "acknowledge equal rows" in {
    val l1 = getExampleLongitudinal("id")
    val l2 = getExampleLongitudinal("id")

    println(l1.valSeq.hashCode)
    println(l2.valSeq.hashCode)
    assert(l1 == l2)
  }
}
