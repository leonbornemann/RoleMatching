package de.hpi.role_matching.compatibility.graph.representation

trait EdgeWeightedSubGraph {

  val scoreRangeIntMin = -10000.0
  val scoreRangeIntMax = 10000.0
  val scoreRangeDoubleMin = -10.0.toFloat
  val scoreRangeDoubleMax = 10.0.toFloat
  val edgeNotPResentValue = -1000000

  //Tranfer x from scale [a,b] to y in scale [c,d]
  // (x-a) / (b-a) = (y-c) / (d-c)
  //
  //y = (d-c)*(x-a) / (b-a) +c
  def scaleInterpolation(x: Double, a: Double, b: Double, c: Double, d: Double) = {
    val y = (d-c)*(x-a) / (b-a) +c
    assert(y >=c && y <= d)
    y
  }

  def getScoreAsInt(weight:Float):Int = {
    if(!(weight==Float.MinValue || weight >= scoreRangeDoubleMin && weight <= scoreRangeDoubleMax))
      println(weight)
    assert( weight==Float.MinValue || weight >= scoreRangeDoubleMin && weight <= scoreRangeDoubleMax)
    val scoreAsInt = if(weight==Float.MinValue) edgeNotPResentValue else scaleInterpolation(weight,scoreRangeDoubleMin,scoreRangeDoubleMax,scoreRangeIntMin,scoreRangeIntMax).round.toInt
    scoreAsInt
  }

}
