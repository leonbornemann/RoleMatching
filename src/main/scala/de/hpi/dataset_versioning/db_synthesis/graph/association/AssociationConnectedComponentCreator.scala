package de.hpi.dataset_versioning.db_synthesis.graph.association

import de.hpi.dataset_versioning.db_synthesis.graph.association.AssociationMergeabilityGraphExplorationMain.subdomain
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import java.io.{File, PrintWriter}
import scala.reflect.io.Directory

class AssociationConnectedComponentCreator(subdomain: String) {

  def create() = {
    val graphRead = AssociationMergeabilityGraph.readFromStandardFile(subdomain)
    //delete old connected component files:
    new Directory(new File(DBSynthesis_IOService.CONNECTED_COMPONENT_DIR(subdomain))).deleteRecursively()
    var connectedComponentCounter = 0
    graphRead
      .toScalaGraph
      .componentTraverser()
      .foreach(c => {
        val nodes = Set() ++ c.nodes.map(_.value)
        val pr = new PrintWriter(DBSynthesis_IOService.CONNECTED_COMPONENT_FILE(subdomain,connectedComponentCounter))
        nodes.foreach(id => pr.println(id.compositeID))
        pr.close()
        connectedComponentCounter+=1
      })
  }

}
