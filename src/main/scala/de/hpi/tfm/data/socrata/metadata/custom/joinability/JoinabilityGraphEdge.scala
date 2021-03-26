package de.hpi.tfm.data.socrata.metadata.custom.joinability

case class JoinabilityGraphEdge(scrDSID:Int,scrColID:Short,targetDSID:Int,targetColID:Short,highestThreshold:Float,highestUniqueness:Float)

object JoinabilityGraphEdge {
  def create(str: String) = {
    val tokens = str.split(",")
    JoinabilityGraphEdge(tokens(0).toInt,tokens(1).toShort,tokens(2).toInt,tokens(3).toShort,tokens(4).toFloat,tokens(5).toFloat)
  }

}
