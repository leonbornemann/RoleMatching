package de.hpi.role_matching.evaluation

import de.hpi.role_matching.compatibility.graph.representation.slim.SlimGraphWithoutWeight

import java.time.LocalDate

case class ValidEdgeCounter(graph: SlimGraphWithoutWeight, trainTimeEnd: LocalDate) extends StatComputer {

  def printCount() = {
    val lineageMap = graph.verticesOrdered.map(_.factLineage.toFactLineage)
    var truePositiveCount = 0
    var trueButUninterestingCount = 0
    var invalidEdgeCount = 0
    graph.adjacencyList.foreach{case (originID,list) => {
      list
        .withFilter(t => t._2.size == graph.trainTimeEnds.size)
        .foreach{case (targetID,_) => {
          val l1 = lineageMap(originID)
          val l2 = lineageMap(targetID)
          val isValid = l1.tryMergeWithConsistent(l2).isDefined
          val evidenceInThisEdge = if(isValid) getEvidenceInTestPhase(l1, l2, trainTimeEnd) else 0
          if(isValid && evidenceInThisEdge>0){
            truePositiveCount+=1
          } else if(isValid){
            trueButUninterestingCount +=1
          } else if(!isValid){
            invalidEdgeCount +=1
          } else {
            println("whoops=")
            throw new AssertionError() //should never end up here
          }
        }}
    }}
    println("truePositiveCount:"+truePositiveCount)
    println("trueButUninterestingCount:"+trueButUninterestingCount)
    println("invalidEdgeCount:"+invalidEdgeCount)
    println(s"$truePositiveCount")
  }

}
