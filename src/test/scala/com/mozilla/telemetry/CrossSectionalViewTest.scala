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

  "CrossSectional" must "be calculated correctly" in {
    val sparkConf = new SparkConf().setAppName("CrossSectionalTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val longitudinalDataset = Seq(
      Longitudinal("a", Option(Seq("DE", "DE", "IT")), Option(Seq(2, 3, 4))),
      Longitudinal("b", Option(Seq("EG", "EG", "DE")), Option(Seq(1, 1, 2)))
    ).toDS

    val actual = longitudinalDataset.map(xx => new CrossSectional(xx))
    val expected = Seq(
      CrossSectional("a", Option("DE")),
      CrossSectional("b", Option("EG"))).toDS

    assert(compareDS(actual, expected))
    sc.stop()
  }
}
