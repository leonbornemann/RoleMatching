package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.AttributeLineage
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd.FunctionalDependencySet
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TemporalTableDecomposer(subdomain: String, id: String,versionHistory:DatasetVersionHistory) {

  val temporalSchema = TemporalSchema.load(id)
  val attrLineageByID = temporalSchema.attributes
    .map(al => (al.attrId,al))
    .toMap
  val latestVersion = versionHistory.latestChangeTimestamp
  val decomposedTablesAtLastTimestamp = DecomposedTable.load(subdomain,id,latestVersion)
  val coveredAttributeLineages = decomposedTablesAtLastTimestamp
    .flatMap(dt => dt.attributes.map(_.id))
    .toSet
    .map(id => attrLineageByID(id))
  val unassignedAttributes = temporalSchema
    .attributes
    .filter(al => !coveredAttributeLineages.contains(al))
    .toSet
  //we need to assign all unassigned attribute ids to a decomposed table
  val freeAttributeAssignment = assignFreeAttributes()


  def getPrimaryKeyPositions(dt: DecomposedTable, curSchema: collection.Map[Int, Attribute]) = {
    val positions = dt.primaryKey
      .filter(pkA => curSchema.contains(pkA.id))
      .map(pkA => {
        curSchema(pkA.id).position.get
      })
    val leftMostPosition = positions.min
    val medianPosition = positions.toSeq.sortWith(_ < _).drop(positions.size/2).head
    val avgPosition = positions.sum / positions.size.toDouble
    (leftMostPosition,medianPosition,avgPosition)
  }

  def scoreDecomposedTableMatch(attrId: Int, curSchema: collection.Map[Int,Attribute], decomposedTables: collection.IndexedSeq[DecomposedTable]) = {
    if(!curSchema.contains(attrId))
      decomposedTables.map(dt => (dt,0))
        .toMap
    else {
      val attrPos = curSchema(attrId).position.get
      val withPosition = decomposedTables.map( dt => (getPrimaryKeyPositions(dt,curSchema),dt) )
        .sortBy(_._1)
      val scores = mutable.HashMap[DecomposedTable,Int]()
      //we want to score the last one that is smaller than attrPos
      val allBigger = withPosition
        .filter(_._1._1>attrPos)
      if(allBigger.size==0){
        //we score the first
        withPosition.zipWithIndex
          .foreach{case ((_,dt),i) => {
            if(i==0) scores.put(dt,1)
            else scores.put(dt,0)
          }}
      } else{
        withPosition.foreach{case (_,dt) => {
          if(allBigger.last._2==dt) scores.put(dt,1)
          else scores.put(dt,0)
        }}
      }
      scores
    }
  }

  def getBestMatchingDecomposedTable(al: AttributeLineage, matches: ArrayBuffer[DecomposedTable]): (DecomposedTable,Boolean) = {
    val addAsKey = matches.size==0
    if(matches.size==1)
      (matches.head,false)
    else{
      val tablesToScore = if(matches.isEmpty) decomposedTablesAtLastTimestamp.toIndexedSeq else matches
      val dtScores = mutable.HashMap[DecomposedTable,Double]() ++ tablesToScore.map((_,0.0))
      versionHistory.versionsWithChanges
        .foreach(v => {
          val curSchema = temporalSchema.valueAt(v)
            .map(t => (t._2))
            .withFilter(a => a.exists)
            .map(a => (a.attr.get.id,a.attr.get))
            .toMap
          val scores = scoreDecomposedTableMatch(al.attrId,curSchema,tablesToScore)
          scores.foreach{case (dt,score) => dtScores(dt) = dtScores(dt) + score}
        })
      val bestTableMatch = dtScores.toIndexedSeq
        .sortBy(_._2)
        .last
        ._1
      (bestTableMatch,addAsKey)
    }
  }

  def getClosedFDs(fdsThisVersion: FunctionalDependencySet, dtByLHS: Map[IndexedSeq[Int], DecomposedTable], curSchema: Map[Int, Attribute]) = {
    val toFind = unassignedAttributes.map(_.attrId)
      .filter(curSchema.contains(_))
    val relevantForClosure = fdsThisVersion.fds
      .withFilter(_._2.intersect(toFind).size!=0)
    dtByLHS.keySet.map(lhs => {
      val lhsSet = lhs.toSet
      var curRHS = mutable.HashSet[Int]() ++ dtByLHS(lhs).attributes.map(_.id)
      relevantForClosure.foreach{case (l,r) => {
        if(l.subsetOf(lhsSet)){
          curRHS.addAll(r)
        }
      }}
      (dtByLHS(lhs),curRHS)
    }).toMap
  }

  def assignFreeAttributes() : Map[DecomposedTable,Set[(AttributeLineage,Boolean)]] = {
    //TODO: we should just iterate over unassignedAttributes!
    val dtByLHS = decomposedTablesAtLastTimestamp
      .map(dt => (dt.sortedPrimaryKeyColIDs,dt))
      .toMap
    val potentialFits = mutable.HashMap[AttributeLineage,mutable.ArrayBuffer[DecomposedTable]]()
      //TODO: we need to get all fds that always contain this al.id
    val foundInitial = mutable.HashMap() ++ unassignedAttributes.map((_,false))
      .toMap
    versionHistory.versionsWithChanges
      .withFilter(v =>  v != latestVersion)
      .foreach(version => {
        val curSchema = temporalSchema.valueAt(version)
          .withFilter(_._2.exists)
          .map(t => (t._2.attr.get.id,t._2.attr.get))
          .toMap
        val fdsThisVersion = FunctionalDependencySet.load(subdomain,id,version)
        val closedKeyFDs = getClosedFDs(fdsThisVersion,dtByLHS,curSchema)
        foundInitial
          .withFilter{case (al,_) => curSchema.contains(al.attrId)}
          .foreach{case (al,foundFirst) => {
            if(!foundFirst){
              closedKeyFDs.foreach{case (dt,rhs) => {
                if(rhs.contains(al.attrId)) {
                  val list = potentialFits.getOrElseUpdate(al, mutable.ArrayBuffer[DecomposedTable]())
                  list.addOne(dt)
                }
              }}
              foundInitial(al) = true
            } else{
              val filtered = potentialFits.getOrElse(al,mutable.ArrayBuffer[DecomposedTable]())
                .filter(dt => closedKeyFDs(dt).contains(al.attrId))
              potentialFits(al) = filtered
            }
        }}
      })
    //potential fits are now updated - we need to chose where to put the atrribute lineage - maybe clostest average distance to primary key occurence (?)
    val resultByAttributeLineage = potentialFits.map{case (al,matches) => (al,getBestMatchingDecomposedTable(al,matches))}
    val result = resultByAttributeLineage.toIndexedSeq
      .groupMap(k => k._2._1)(v => (v._1,v._2._2))
      .map{case (k,v) => (k,v.toSet)}
    if(!unassignedAttributes.forall(resultByAttributeLineage.contains(_))){
      throw new AssertionError("we should always find something for an attribute lineage")
    }
    result
  }


  def createDecomposedTemporalTables() = {
    decomposedTablesAtLastTimestamp.foreach(dt => {
      val extraKeyAttributes = freeAttributeAssignment.getOrElse(dt,Set())
        .filter(_._2)
        .map(_._1)
      val extraNonKeyAttributes = freeAttributeAssignment.getOrElse(dt,Set())
        .filter(!_._2)
        .map(_._1)
      val containedAttrLineages = (dt.attributes.map(a => attrLineageByID(a.id))
        ++ extraNonKeyAttributes)
        .sortBy(_.lastDefinedValue.position.getOrElse(Int.MaxValue))
      val originalFDLHS = dt.primaryKey.map(a => attrLineageByID(a.id))
      val primaryKey = dt.primaryKey.map(a => attrLineageByID(a.id)) ++ extraKeyAttributes//TODO
      //build pk info:
      val pkByTImestampMap = versionHistory.versionsWithChanges
        .withFilter(v => originalFDLHS.exists(_.valueAt(v)._2.exists))
        .map(v => {
          val extraKeyAttributesThisVersion = extraKeyAttributes.filter(_.valueAt(v)._2.exists)
            .map(_.valueAt(v)._2.attr.get)
          val curPk = (originalFDLHS
            .withFilter(_.valueAt(v)._2.exists)
            .map(_.valueAt(v)._2.attr.get)
            ++ extraKeyAttributesThisVersion)
          (v,curPk)
      }).toMap
      val foreignKey = dt.foreignKeys.map(a => attrLineageByID(a.id))
      val decomposedTemporalTable = DecomposedTemporalTable(subdomain,dt.originalID,dt.id,containedAttrLineages,originalFDLHS,pkByTImestampMap)
      decomposedTemporalTable.writeToStandardFile()
    })

  }


}
