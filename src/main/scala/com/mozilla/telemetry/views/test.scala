package com.mozilla.telemetry.views

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext 

object TestRead {
  val sparkConf = new SparkConf().setAppName("l10l Read Example")
  sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
  implicit val sc = new SparkContext(sparkConf)
  val hiveContext = new HiveContext(sc)
  import hiveContext.implicits._

  def main(args: Array[String]): Unit = {
	println("=+"*40)
    val frame = hiveContext.sql("SELECT * FROM longitudinal").limit(10)
	println("=+"*40)
    val ds = frame.as[longitudinal]
	println("=+"*40)
    //val output = ds.map(CrossSectionalView.modalCountry)
    //println(output.count)
  }
}
