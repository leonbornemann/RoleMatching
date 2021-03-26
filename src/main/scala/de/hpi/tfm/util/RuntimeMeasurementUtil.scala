package de.hpi.tfm.util

object RuntimeMeasurementUtil {

  def executionTimeInSeconds[R](block: => R): (R,Double) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val resultTime = (t1-t0)/1000000000.0
    (result,resultTime)
  }

}
