package de.hpi.role_matching.cbrm.compatibility_graph

import java.time.LocalDate

case class RunConfig(decayMethod:String,decayProbability:Option[Double],trainTimeEnd:LocalDate){

}
object RunConfig{

  def fromString(configString: String) = {
    val tokens = configString.split("_")
    if(configString.startsWith("NO_DECAY")){
      RunConfig("NO_DECAY",None,LocalDate.parse(tokens.last))
    } else {
      RunConfig("PROBABILISTIC_DECAY_FUNCTION",Some(tokens(3).toDouble),LocalDate.parse(tokens.last))
    }
  }
}
