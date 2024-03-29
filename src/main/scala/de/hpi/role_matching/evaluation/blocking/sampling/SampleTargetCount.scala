package de.hpi.role_matching.evaluation.blocking.sampling

case class SampleTargetCount(var below70:Int, var below100:Int, var fullCompatibility:Int){

  def reduceNeededCount(compatibility: Double) = {
    if(compatibility < firstThreshold)
      below70-=1
    else if(compatibility<secondThreshold)
      below100-=1
    else
      fullCompatibility-=1
  }

  val firstThreshold = 0.7
  val secondThreshold = 1.0

  def stillNeeds(compatibility: Double): Boolean = {
    if(compatibility < firstThreshold)
      below70>0
    else if(compatibility<secondThreshold)
      below100>0
    else
      fullCompatibility>0
  }

  def needsMoreSamples = below70 > 0 || below100 > 0 || fullCompatibility > 0
}

object SampleTargetCount {
  def fromArray(remainingNeededSamples: Array[Int]) = {
    assert(remainingNeededSamples.size==3)
    new SampleTargetCount(remainingNeededSamples(0),remainingNeededSamples(1),remainingNeededSamples(2))
  }

}
