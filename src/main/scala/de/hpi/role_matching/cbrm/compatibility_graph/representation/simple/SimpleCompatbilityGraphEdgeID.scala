package de.hpi.role_matching.cbrm.compatibility_graph.representation.simple

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}

case class SimpleCompatbilityGraphEdgeID(v1:String,v2:String) extends JsonWritable[SimpleCompatbilityGraphEdge]{

}

object SimpleCompatbilityGraphEdgeID extends JsonReadable[SimpleCompatbilityGraphEdgeID]
