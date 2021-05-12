import de.hpi.tfm.data.socrata.change.ReservedChangeValues
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.{RuzickaDistanceComputer, TransitionHistogramMode}
import de.hpi.tfm.io.IOService

import java.time.LocalDate

object RuzickaSimilarityTest extends App {
  val f1 = "A__B__B_D"
  val f2 = "AABBBCBDD"
  val begin = LocalDate.parse("2010-01-01")
  val end = begin.plusDays(f1.size-1)
  IOService.STANDARD_TIME_FRAME_START=begin
  IOService.STANDARD_TIME_FRAME_END=end
  val computer = new RuzickaDistanceComputer(strToFactLineage(f1),
    strToFactLineage(f2),
    1,
    TransitionHistogramMode.NORMAL)
  println(computer.computeScore())

  def strToFactLineage(str:String) = {
    val withTime = str
      .zipWithIndex
      .map{case (c,i) => {
        val value = if(c=='_') ReservedChangeValues.NOT_EXISTANT_CELL else c.toString
        (begin.plusDays(i-1),value)
      }}
      .zipWithIndex
    val asMap = collection.mutable.TreeMap[LocalDate, Any]() ++ withTime
      .filter { case ((t, v), i) => i == 0 || v != withTime(i-1)._1._2 }
      .map(_._1)
    FactLineage(asMap)
  }

}
