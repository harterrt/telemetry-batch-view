package com.mozilla.telemetry


import com.mozilla.telemetry.views.{CrossSectionalView, longitudinal}
import org.scalatest.{FlatSpec}

class CrossSectionalViewTest extends FlatSpec {
  val fixture = {
  }

  "Modes" must "combine repeated keys" in {
    val ll = longitudinal("id", Seq("DE", "IT", "DE", "DE"), Seq(1, 2, 1, 1))
    val country = CrossSectionalView.modalCountry(ll)
    assert(country == "DE")
  }

  "Modes" must "respect session weight" in {
    val ll = longitudinal("id", Seq("DE", "IT"), Seq(3, 1))
    val country = CrossSectionalView.modalCountry(ll)
    assert(country == "DE")
  }
}
