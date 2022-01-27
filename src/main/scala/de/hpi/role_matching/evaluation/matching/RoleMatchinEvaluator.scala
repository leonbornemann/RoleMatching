package de.hpi.role_matching.evaluation.matching

import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.MemoryEfficientCompatiblityGraphSet
import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleLineageWithID, Roleset}
import de.hpi.role_matching.cbrm.sgcp.{RoleMerge, ScoreConfig}

import java.io.PrintWriter
import java.time.LocalDate

case class RoleMatchinEvaluator(prCliques: PrintWriter,
                                prCliquesTruePositivesToReview: PrintWriter,
                                prCliquesRestToReview: PrintWriter,
                                prTableStrings: PrintWriter,
                                prEdges:PrintWriter,
                                vertexLookupMap: Roleset,
                                trainTimeEnd: LocalDate,
                                slimGraphSet:Option[MemoryEfficientCompatiblityGraphSet],
                                scoreConfig: Option[ScoreConfig],
                                maxRecalEdgeIds:Option[Set[String]]=None) extends StatComputer {

  //    val vertexLookupMap = VertexLookupMap.fromJsonFile(args(1) + s"/education.json").getStringToLineageMap

  var totalValidEdges = 0
  var totalValidEdgesThatAlsoAreInMaxRecallSet = 0
  var totalInvalidEdges = 0
  var correctCliques = 0
  var incorrectCliques = 0

  val indexOfTrainTimeEnd = slimGraphSet.map(_.trainTimeEnds.indexOf(trainTimeEnd))

  def serializeSchema() = {
    prCliques.println("ComponentID,Method,cliqueID,cliqueSize,edgesTotal,validEdges,totalEvidence,fractionOfVerticesWithEvidence,score,alpha") //cliqueID is specific per method
    prEdges.println("ComponentID,Method,cliqueID,cliqueSize,vertex1ID,vertex2ID,remainsValid,evidence,score")
    prCliquesTruePositivesToReview.println("ComponentID,Method,cliqueID,cliqueRoleIDs,evidence,commonValues")
    prCliquesRestToReview.println("ComponentID,Method,cliqueID,cliqueRoleIDs,evidence,commonValues")
  }

  def getScore(i: Int, j: Int) = {
    if(scoreConfig.isDefined){
      val score = scoreConfig.get.computeScore(slimGraphSet.get.adjacencyList(i)(j)(indexOfTrainTimeEnd.get))
      score
    } else {
      Float.NaN
    }
  }

  def toCSVSafe(str: Any) = {
    val string = if(str==null) "null" else str
    string.toString.replace("\r"," ").replace("\n"," ").replace(","," ")
  }

  def getCommonValuesAsSemicolonSeparatedString(vertices: IndexedSeq[RoleLineageWithID]) = {
    val allDates = vertices.flatMap(_.factLineage.lineage.keySet)
    val valuesAtDate = allDates.map(ld => {
      (ld,vertices.map(_.factLineage.toRoleLineage.valueAt(ld)))
    })
    val values = valuesAtDate
      .filter{case (ld,values) => values.filter(v => !RoleLineage.isWildcard(v)).size>1}
      .sortBy(-_._2.size)
      .take(10)
      .map(t => toCSVSafe(t._2.head))
    values.mkString(";")
  }

  def addResultTuple(c: RoleMerge, componentID: String, method: String) = {
    val verticesSorted = c.clique.toIndexedSeq.sorted
    val cliqueID = verticesSorted.head
    val vertices = verticesSorted.map(i => vertexLookupMap.posToRoleLineage(i))
    val identifiedVertices = verticesSorted.map(i => vertexLookupMap.positionToRoleLineage(i))
    var evidenceCountTotal = 0
    var validEdges = 0
    var edgesTotal = 0
    val hasEvidence = collection.mutable.HashMap[Int, Boolean]()
    for (i <- 0 until vertices.size) {
      for (j <- (i + 1) until vertices.size) {
        edgesTotal += 1
        val l1 = vertices(i)
        val l2 = vertices(j)
        val evidenceInThisEdge = getEvidenceInTestPhase(l1, l2, trainTimeEnd)
        evidenceCountTotal += evidenceInThisEdge
        val scoreThisEdge = if(slimGraphSet.isDefined) getScore(verticesSorted(i),verticesSorted(j)) else -1.0
        if (evidenceInThisEdge > 0) {
          hasEvidence.put(i, true)
          hasEvidence.put(j, true)
        }
        var remainsValid = false
        if (l1.tryMergeWithConsistent(l2).isDefined) {
          validEdges += 1
          remainsValid=true
        }
        assert(i<j)
        val vertexID1 = identifiedVertices(i).csvSafeID
        val vertexID2 = identifiedVertices(j).csvSafeID
        assert(verticesSorted(i)<verticesSorted(j))
        assert(vertexID1<vertexID2)
        //    df['edgeID'] = df['vertex1ID'] + '_' + df['vertex2ID']
        if(maxRecalEdgeIds.isDefined){
          val edgeID = vertexID1 + "_" + vertexID2
          val evidenceInTrainPhase = getEvidenceInTrainPhase(l1, l2, trainTimeEnd)
          if(evidenceInThisEdge>0 && evidenceInTrainPhase>0){
            if(remainsValid ) {
              totalValidEdges+=1
              if(maxRecalEdgeIds.get.contains(edgeID))
                totalValidEdgesThatAlsoAreInMaxRecallSet+=1
            } else
              totalInvalidEdges+=1
          }
        }
        prEdges.println(s"$componentID,$method,$cliqueID,${c.clique.size},$vertexID1,$vertexID2,$remainsValid,$evidenceInThisEdge,$scoreThisEdge") //TODO: compute score?
      }
    }
    if(evidenceCountTotal>0){
      if(validEdges == edgesTotal){
        correctCliques+=1
      } else{
        incorrectCliques+=1
      }
    }
    val verticesWithAtLeastOneEdgeWithEvidence = hasEvidence.values.filter(identity).size
    val fractionOfVerticesWithEvidence = verticesWithAtLeastOneEdgeWithEvidence / vertices.size.toDouble
    prCliques.println(s"$componentID,$method,$cliqueID,${c.clique.size},$edgesTotal,$validEdges,$evidenceCountTotal,$fractionOfVerticesWithEvidence,${c.cliqueScore},${scoreConfig.map(_.alpha).getOrElse(0.0f)}")
    val commonValues = getCommonValuesAsSemicolonSeparatedString(identifiedVertices)
    if(identifiedVertices.size>1 && identifiedVertices.size<=25 && validEdges==edgesTotal && evidenceCountTotal>1){
      val idsInClique = identifiedVertices.map(id => id.csvSafeID).mkString(";")
      val tableString = RoleLineageWithID.getTabularEventLineageString(identifiedVertices)
      prCliquesTruePositivesToReview.println(s"$componentID,$method,$cliqueID,$evidenceCountTotal,$idsInClique,$commonValues")
      prTableStrings.println(tableString)
    } else {
      val idsInClique = identifiedVertices.map(id => id.csvSafeID).mkString(";")
      prCliquesRestToReview.println(s"$componentID,$method,$cliqueID,$evidenceCountTotal,$idsInClique,$commonValues")
    }
  }

  def addResultTuples(cliquesGreedy: collection.Seq[RoleMerge], componentID: String, method: String) = {
    cliquesGreedy.foreach(c => addResultTuple(c, componentID, method))
  }

  def printResults() = {
    println(s"totalValidEdges: $totalValidEdges")
    println(s"totalInvalidEdges: $totalInvalidEdges")

    if(maxRecalEdgeIds.isDefined){
      println(s"totalValidEdgesThatAlsoAreInMaxRecallSet: $totalValidEdgesThatAlsoAreInMaxRecallSet")
      println(s"maxRecallSetSize: ${maxRecalEdgeIds.get.size}")
    }

    println(s"correctCliques: $correctCliques")
    println(s"incorrectCliques: $incorrectCliques")
  }


}
