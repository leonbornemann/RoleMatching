package de.hpi.role_matching.cbrm.compatibility_graph.representation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.CompatibilityGraphEdge
import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}

case class RoleCompatibilityGraph(edges: IndexedSeq[CompatibilityGraphEdge], graphConfig: GraphConfig) extends JsonWritable[RoleCompatibilityGraph]{

  //discard evidence to save memory
  def withoutEvidenceSets = RoleCompatibilityGraph(edges
    .map(e => CompatibilityGraphEdge(e.tupleReferenceA,e.tupleReferenceB,e.evidence,None)),graphConfig)

}
object RoleCompatibilityGraph extends JsonReadable[RoleCompatibilityGraph] with StrictLogging{


}
