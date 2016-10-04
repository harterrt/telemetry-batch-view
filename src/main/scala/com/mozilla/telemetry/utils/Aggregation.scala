package com.mozilla.telemetry.utils

package object aggregation {
  private def validateWeights[A](values: Seq[A], weights: Seq[Long]) = {
    if (values.size != weights.size) {
      throw new IllegalArgumentException("Weights and values must have the same length.")
    }
    if (values.size == 0) {
      throw new IllegalArgumentException("Weights and values must have length > 0.")
    }
    if (weights.sum <= 0) {
      throw new IllegalArgumentException("Weights must have positive sum")
    }

    true
  }

  def mean(values: Seq[Long]) = {
    if (values.size > 0) {
      Some(values.sum.toDouble / values.size)
    } else {
      None
    }
  }

  def weightedMean(values: Seq[Option[Long]], weights: Seq[Long]): Double = {
    validateWeights(values, weights)
    val clean_pairs = (values zip weights).map(x => x match{
      case (Some(v), w) => Some((v, w))
      case _ => None
    }).flatten
    clean_pairs.foldLeft(0.0)((acc, pair) => acc + (pair._1 * pair._2)) / weights.sum
  }

  def weightedMode[A](values: Seq[A], weights: Seq[Long]): A = {
    validateWeights(values, weights)
    val pairs = values zip weights
    val agg = pairs.groupBy(_._1).map(kv => (kv._1, kv._2.map(_._2).sum))
    agg.maxBy(_._2)._1
  }
}

