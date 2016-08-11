package com.mozilla.telemetry


import com.mozilla.telemetry.views._
import CrossSectionalView._
import org.scalatest.{FlatSpec}
import org.apache.spark.sql.Dataset

import sqlContext.implicits._


class CrossSectionalViewTest extends FlatSpec {
  val longitudinalDataset = Seq(
    longitudinal("a", Seq("DE", "DE", "IT"), Seq(2, 3, 4)),
    longitudinal("b", Seq("EG", "EG", "DE"), Seq(1, 1, 2))
  ).toDS

  def compareDS(actual: Dataset[crossSectional], expected: Dataset[crossSectional]) = {
    actual.collect.zip(expected.collect)
      .map(xx => xx._1 == xx._2)
      .reduce(_ && _)
  }

  "CrossSectional" must "be calculated correctly" in {
    val expected = Seq(crossSectional("a", "DE"), crossSectional("b", "EG")).toDS
    val actual = longitudinalDataset.map(generateCrossSectional)

    assert(compareDS(actual, expected))
  }

  "Modes" must "combine repeated keys" in {
    val ll = longitudinal("id", Seq("DE", "IT", "DE"), Seq(3, 6, 4))
    val country = modalCountry(ll)
    assert(country == "DE")
  }

  it must "respect session weight" in {
    val ll = longitudinal("id", Seq("DE", "IT", "IT"), Seq(3, 1, 1))
    val country = modalCountry(ll)
    assert(country == "DE")
  }
}
