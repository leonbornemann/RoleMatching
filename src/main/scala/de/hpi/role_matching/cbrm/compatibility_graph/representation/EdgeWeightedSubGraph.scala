package de.hpi.role_matching.cbrm.compatibility_graph.representation

trait EdgeWeightedSubGraph {

  val scoreRangeIntMin = -500.0
  val scoreRangeIntMax = 500.0
  var scoreRangeDoubleMin = -10.0.toFloat
  var scoreRangeDoubleMax = 10.0.toFloat
  val edgeNotPResentValue = -1000000

  //Tranfer x from scale [a,b] to y in scale [c,d]
  // (x-a) / (b-a) = (y-c) / (d-c)
  //
  //y = (d-c)*(x-a) / (b-a) +c
  def scaleInterpolation(x: Double, a: Double, b: Double, c: Double, d: Double) = {
    val y = (d-c)*(x-a) / (b-a) +c
    if(!(y >=c && y <= d))
      println()
    assert(y >=c && y <= d)
    y
  }

  def getScoreAsInt(weight:Double):Int = {
    if(!(weight==Double.MinValue || weight >= scoreRangeDoubleMin && weight <= scoreRangeDoubleMax))
      println(weight)
    assert( weight==Double.MinValue || weight >= scoreRangeDoubleMin && weight <= scoreRangeDoubleMax)
    val scoreAsInt = if(weight==Double.MinValue) edgeNotPResentValue else {
      val interpolated = scaleInterpolation(weight,scoreRangeDoubleMin,scoreRangeDoubleMax,scoreRangeIntMin,scoreRangeIntMax)
      if(interpolated<1.0 && interpolated>0.0)
        interpolated.ceil.toInt
      else if(interpolated> -1.0 && interpolated < 0.0)
        interpolated.floor.toInt
      else
        interpolated.round.toInt
    }
    scoreAsInt
  }

}
