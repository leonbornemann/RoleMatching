package de.hpi.dataset_versioning.data.metadata.custom.joinability

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.io.Source

class JoinabilityGraph() extends StrictLogging{
  def numEdges() = {
    if(groupedByDSRepresentation)
      adjacencyListGroupedByDS.mapValues(_.mapValues(_.size).values.sum).values.sum
    else
      adjacencyListGroupedByDSAndCol.values.map(_.size).sum
  }


  val NO_CONTAINMENT_VAL: Float = Float.NegativeInfinity

  private def ensureConsistency(minThreshold1Direction:Float = 1.0f) = {
    val edgesToPrune = mutable.HashSet[((Int,LocalDate),(Int,LocalDate),Short,Short)]()
    val toAdd = mutable.HashSet[((Int,LocalDate),(Int,LocalDate),Short,Short)]()
    for (ds1 <- adjacencyListGroupedByDS.keySet) {
      for ((ds2, edges) <- adjacencyListGroupedByDS(ds1)) {
        val ds2Connections = adjacencyListGroupedByDS.getOrElse(ds2, mutable.HashMap[(Int,LocalDate), mutable.Map[ColEdge, Float]]())
        val ds2Tods1 = ds2Connections.getOrElse(ds1, mutable.Map[ColEdge, Float]())
        edges.foreach { case (ColEdge(c1, c2), f) => {
          val otherEdgeExists = adjacencyListGroupedByDS.contains(ds2) && ds2Connections.contains(ds1) && ds2Tods1.contains(ColEdge(c2,c1))
          val otherVal = if(otherEdgeExists) ds2Tods1(ColEdge(c2,c1)) else NO_CONTAINMENT_VAL
          if(f<minThreshold1Direction && otherVal<minThreshold1Direction){
            edgesToPrune.add( (ds1,ds2,c1,c2))
          }
          if(!otherEdgeExists){
            toAdd.add((ds2,ds1,c2,c1))
          }
        }
        }
      }
    }
    logger.debug(s"Found ${toAdd.size} edges to add")
    logger.debug(s"Found ${edgesToPrune.size} edges to remove")
    // adding missing edges:
    toAdd.foreach{case (srcDs,targetDs,srcCol,targetCol) => {
      val colPairToContainment = adjacencyListGroupedByDS.getOrElseUpdate(srcDs,mutable.HashMap[(Int,LocalDate), mutable.Map[ColEdge, Float]]())
        .getOrElseUpdate(targetDs,mutable.HashMap[ColEdge, Float]())
      assert(!colPairToContainment.contains(ColEdge(srcCol,targetCol)))
      colPairToContainment(ColEdge(srcCol,targetCol)) = NO_CONTAINMENT_VAL
    }}
    //pruning superfluous edges:
    logger.debug("Starting to prune graph")
    logger.debug(s"pruning ${edgesToPrune.size*2} edges out of ${adjacencyListGroupedByDS.mapValues(_.mapValues(_.size).values.sum).values.sum}")
    edgesToPrune.foreach{case (srcDs,targetDS,srcCol,targetCol) => {
      removeBothEdges(srcDs,targetDS,srcCol,targetCol)
    }}
  }

  private def assertIntegrity() = {
    adjacencyListGroupedByDSAndCol.foreach{case (k1,edges) => {
      edges.foreach{ case (k2,_) => {
        val a = adjacencyListGroupedByDSAndCol.getOrElse(k2,Map[DatasetColumnVertex, Float]())
        assert(a.contains(k1))
      }}
    }}
  }


  def switchToAdjacencyListGroupedByDSAndCol() = {
    var a = adjacencyListGroupedByDS.exists{ case (a,b) => {
      b.exists{case (c,d) => a._2.isAfter(c._2)}
    }}
    logger.debug("has Relevant edges")
    logger.debug(s"$a")
    for (ds1 <- adjacencyListGroupedByDS.keySet) {
      for ((ds2, edges) <- adjacencyListGroupedByDS(ds1)) {
        edges.foreach{ case (ColEdge(c1,c2),f) => {
          val map = adjacencyListGroupedByDSAndCol.getOrElseUpdate(DatasetColumnVertex(ds1._1,ds1._2,c1),mutable.HashMap[DatasetColumnVertex,Float]())
          map.put(DatasetColumnVertex(ds2._1,ds2._2,c2),f)
        }}
      }
    }
    adjacencyListGroupedByDS.clear()
    a = adjacencyListGroupedByDSAndCol.exists{case (a,b) => b.exists{case (c,d) => a.version.isAfter(c.version)}}
    logger.debug("has Relevant edges")
    logger.debug(s"$a")
  }

  private def setEdgeValue(srcDS: (Int,LocalDate), targetDS: (Int,LocalDate), scrColID: Short, targetColID: Short, threshold: Float) = {
    //one-way
    val map = adjacencyListGroupedByDS.getOrElseUpdate(srcDS,mutable.HashMap())
    .getOrElseUpdate(targetDS,mutable.HashMap())
    map(ColEdge(scrColID,targetColID)) = threshold
    //second-way
  }

  private def removeBothEdges(srcDs: (Int,LocalDate), targetDS: (Int,LocalDate), srcCol: Short, targetCol: Short) = {
    removeEdge(srcDs,targetDS,srcCol,targetCol)
    removeEdge(targetDS,srcDs,targetCol,srcCol)
  }

  private def removeEdge(srcDs: (Int,LocalDate), targetDS: (Int,LocalDate), srcCol: Short, targetCol: Short) = {
    if(adjacencyListGroupedByDS.contains(srcDs)){
      val outerMap = adjacencyListGroupedByDS(srcDs)
      if(outerMap.contains(targetDS)){
        val map = outerMap(targetDS)
        map.remove(ColEdge(srcCol,targetCol))
        if(map.isEmpty) outerMap.remove(targetDS)
        if(outerMap.isEmpty) adjacencyListGroupedByDS.remove(srcDs)
      }
    }
  }

  //to speed up getOrElse calls (avoids object creation)
  private val emptyMap = Map[(Int,LocalDate),Map[ColEdge,(Float,Float)]]()
  private val emptyMap2 = Map[ColEdge,Float]()

  private def hasEdge(srcDS: (Int,LocalDate), targetDS: (Int,LocalDate), scrColID: Short, targetColID: Short): Boolean = {
    adjacencyListGroupedByDS.getOrElse(srcDS,emptyMap).getOrElse(targetDS,emptyMap2).contains(ColEdge(scrColID,targetColID))
  }

  def groupedByDSRepresentation: Boolean = !adjacencyListGroupedByDS.isEmpty

  def getEdgeValue(srcDS: (Int,LocalDate), targetDS: (Int,LocalDate), scrColID: Short, targetColID: Short) = {
    if(groupedByDSRepresentation)
      adjacencyListGroupedByDS(srcDS)(targetDS)(ColEdge(scrColID,targetColID))
    else
      adjacencyListGroupedByDSAndCol(DatasetColumnVertex(srcDS._1,srcDS._2,scrColID))(DatasetColumnVertex(targetDS._1,targetDS._2,targetColID))
  }

  val adjacencyListGroupedByDS = mutable.HashMap[(Int,LocalDate),mutable.Map[(Int,LocalDate),mutable.Map[ColEdge,Float]]]()
  val adjacencyListGroupedByDSAndCol = mutable.HashMap[DatasetColumnVertex,mutable.Map[DatasetColumnVertex,Float]]()

}

case class ColEdge(src:Short, target:Short)

case class DatasetColumnVertex(dsID:Int, version:LocalDate, colID:Short)

object JoinabilityGraph extends StrictLogging {

  def toDate(str: String) = LocalDate.parse(str,IOService.dateTimeFormatter)

  def readGraphFromGoOutput(path: File, uniquenessThreshold:Float = Float.NegativeInfinity) = {
    val lineIterator = Source.fromFile(path).getLines()
    val graph = new JoinabilityGraph()
    lineIterator.next()
    logger.debug("Beginning to read graph")
    while(lineIterator.hasNext){
      val tokens = lineIterator.next().split(",")
      //assert(tokens.size==8)
      val (scrDSID,srcVersion,scrColID,targetDSID,targetVersion,targetColID) = (tokens(0).toInt,toDate(tokens(1)),tokens(2).toShort,tokens(3).toInt,toDate(tokens(4)),tokens(5).toShort)
      val (containmentOfSrcInTarget,containmentOfTargetInSrc,maxUniqueness) = (tokens(6).toFloat,tokens(7).toFloat,tokens(8).toFloat)
      if(maxUniqueness >= uniquenessThreshold){
        if(srcVersion.isAfter(targetVersion))
        if(!graph.hasEdge((scrDSID,srcVersion),(targetDSID,targetVersion),scrColID,targetColID))
          graph.setEdgeValue((scrDSID,srcVersion),(targetDSID,targetVersion),scrColID,targetColID,containmentOfSrcInTarget)
        if(!graph.hasEdge((targetDSID,targetVersion),(scrDSID,srcVersion),targetColID,scrColID))
          graph.setEdgeValue((targetDSID,targetVersion),(scrDSID,srcVersion),targetColID,scrColID,containmentOfTargetInSrc)
      }
    }
    logger.debug("Read graph from output - beginning consistency check")
    graph.ensureConsistency()
    logger.debug("Finished consistency check - beginning integrity check")
    graph.assertIntegrity()
    logger.debug("Finished integrity check")
    graph
  }
}
