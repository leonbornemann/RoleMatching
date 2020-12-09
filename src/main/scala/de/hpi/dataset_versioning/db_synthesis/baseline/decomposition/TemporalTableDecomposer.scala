package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, AttributeState, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.fd.FunctionalDependencySet
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TemporalTableDecomposer(subdomain: String, id: String,versionHistory:DatasetVersionHistory) {


  val temporalSchema = TemporalSchema.load(id)
  val attrLineageByID = temporalSchema.attributes
    .map(al => (al.attrId,al))
    .toMap
  val latestVersion = versionHistory.latestChangeTimestamp
  val decomposedTablesAtLastTimestamp = DecomposedTable.load(subdomain,id,latestVersion) //Those should be in-order (?)
  decomposedTablesAtLastTimestamp.foreach(dt => println(dt.getSchemaStringWithIds))
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


  def createSurrogateBasedDtts(dtts: IndexedSeq[DecomposedTemporalTable]) = {
    val allAttrIds = dtts.flatMap(_.containedAttrLineages.map(_.attrId)).toSet
    val idToReferredTables = allAttrIds.map( id => {
      (id,dtts.filter(_.primaryKey.exists(_.attrId == id)))
    }).toMap
    val idToLineages = dtts.flatMap(_.containedAttrLineages)
      .groupBy(_.attrId)
    //assert equality:
    //idToLineages.foreach(t => assert((0 until t._2.size-1).forall(i => t._2(i).lineage==t._2(i+1).lineage)))
    val idToAttribute = idToLineages.map{case (id,group) => (id,group.head)}
    val pkIds = dtts
      .flatMap(_.primaryKey.map(_.attrId)).toSet
    val naturalKeyIDToSurrogateID = pkIds.map(i => {
        val attr = idToAttribute(i)
        val toReturn = (i,(GlobalSurrogateRegistry.getNextFreeSurrogateID,attr))
        (toReturn)
    }).toMap
    val naturalKeyOccurredInTable = mutable.HashMap() ++ naturalKeyIDToSurrogateID.keySet.map(id => (id,false)).toMap
    val surrogateBasedTables = dtts.map(dtt => {
      val newNonKeys = mutable.ArrayBuffer[AttributeLineage]()
      val oldReferences = mutable.ArrayBuffer[(AttributeLineage,collection.IndexedSeq[DecomposedTemporalTableIdentifier])]()
      dtt.containedAttrLineages.foreach(al => {
        val otherReferencedTables = idToReferredTables(al.attrId).filter(_!=dtt)
        if(otherReferencedTables.isEmpty || dtt.primaryKey.contains(al) && !naturalKeyOccurredInTable(al.attrId)) {
          newNonKeys += al
          if(dtt.primaryKey.contains(al))
            naturalKeyOccurredInTable(al.attrId) = true
        }
        else {
          val toAdd = (al,otherReferencedTables.map(_.id))
          oldReferences.addOne(toAdd)
        }
      })
      //dtt.containedAttrLineages.partition(al => idToReferredTables(al.attrId).filter(_.))
      val newKey = dtt.primaryKey.toIndexedSeq
        .map(pk => {
          createSurrogateAttribute(naturalKeyIDToSurrogateID, pk)
        })
      val newReferences = oldReferences.map{case (fk,referredTables) => {
        (createSurrogateAttribute(naturalKeyIDToSurrogateID, fk),referredTables)
      }}.toIndexedSeq.sortBy(_._1.surrogateID)
      new SurrogateBasedDecomposedTemporalTable(dtt.id,newKey,newNonKeys,newReferences)
    })
    assert(surrogateBasedTables.flatMap(_.attributes.map(_.attrId)).size == dtts.flatMap(_.containedAttrLineages).map(_.attrId).toSet.size)
    surrogateBasedTables.foreach(sbdtt => {
      sbdtt.writeToStandardFile()
      val (bcnfReferenceTable,associations) = sbdtt.furtherDecomposeToAssociations
      associations.foreach(sbdta => sbdta.writeToStandardFile())
      //sbdtt.getReferenceSkeleton().writeToStandardFile()
      bcnfReferenceTable.writeToStandardFile()
    })
    //new AttributeLineage(curSurrogateKeyID,mutable.TreeMap[LocalDate,AttributeState](initialInsert -> ),true)
  }

  private def createSurrogateAttribute(naturalKeyIDToSurrogateID: Map[Int, (Int, AttributeLineage)], pk: AttributeLineage) = {
    val (surrogateID, original) = naturalKeyIDToSurrogateID(pk.attrId)
    val newName = original.lastName + s"_SURROGATE_ID[$surrogateID]"
    val newAttributeState = new AttributeState(Some(Attribute(original.lastName + s"_SURROGATE_ID[$surrogateID]", surrogateID, None, None))) //this will always be position 0 now
    new SurrogateAttributeLineage(surrogateID, original.attrId,original.lineage.firstKey)
  }

  def createDecomposedTemporalTables() = {
    val dttToExtraKeyAttributes = mutable.HashMap[DecomposedTemporalTableIdentifier,Set[AttributeLineage]]()
    val tableReferences = mutable.HashMap[DecomposedTemporalTable,collection.Set[Set[AttributeLineage]]]() //maps to the identifying LHS
    val byLHS = decomposedTablesAtLastTimestamp.map(dt => {
      val extraKeyAttributes = freeAttributeAssignment.getOrElse(dt,Set()) //whenever we add something here, we need to update all dtts that refer to this table via fk-relationships
        .filter(_._2)
        .map(_._1)
      val extraNonKeyAttributes = freeAttributeAssignment.getOrElse(dt,Set())
        .filter(!_._2)
        .map(_._1)
      var containedAttrLineages = (dt.attributes.map(a => attrLineageByID(a.id))
        ++ extraNonKeyAttributes)
        .sortBy(_.lastDefinedValue.position.getOrElse(Int.MaxValue))
      //add extra key attributes:
      containedAttrLineages = containedAttrLineages ++ extraKeyAttributes.filter(al => !containedAttrLineages.contains(al))
      val originalFDLHS = dt.primaryKey.map(a => attrLineageByID(a.id))
      val primaryKey = dt.primaryKey.map(a => attrLineageByID(a.id)) ++ extraKeyAttributes//TODO
      //build pk info:
      val pkByTimestampMap = versionHistory.versionsWithChanges
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
      val decomposedTemporalTable = natural_key_based.DecomposedTemporalTable(DecomposedTemporalTableIdentifier(subdomain,dt.originalID,dt.id,None),
        mutable.ArrayBuffer() ++ containedAttrLineages,
        originalFDLHS,
        pkByTimestampMap,
      mutable.HashSet())
      dttToExtraKeyAttributes.put(decomposedTemporalTable.id,extraKeyAttributes)
      val foreignKeyLineages = dt.foreignKeys.map(attrs => attrs.map(a => attrLineageByID(a.id)))
      tableReferences.put(decomposedTemporalTable,foreignKeyLineages)
      (originalFDLHS,decomposedTemporalTable)
    }).toMap
    //update table references and add needed extra attributes
    val allFKsAndPks = byLHS.values.flatMap(_.primaryKey.map(_.attrId)).toSet
    val a = byLHS.values.flatMap(_.containedAttrLineages.filter(dtt => !allFKsAndPks.contains(dtt.attrId)).map(_.attrId).toIndexedSeq).groupBy(identity)
      .filter(_._2.size>1)

    byLHS.values.foreach(dtt => {
      val referencedTablesLHS = tableReferences(dtt)
      val references = referencedTablesLHS.map(lhs => byLHS(lhs))
      dtt.referencedTables.addAll(references.map(_.id))
      //for every reference, add the id to reference tables and the extra key attributes that are needed
      val attributeLineagesToAdd = referencedTablesLHS.flatMap(lhs => {
        val table = byLHS(lhs)
        dttToExtraKeyAttributes(table.id)
      })
      dtt.containedAttrLineages.addAll(attributeLineagesToAdd.diff(dtt.containedAttrLineages.toSet))
      //write to file:
      //dtt.writeToStandardFile()
//      //also create associations:
//      dtt.furtherDecomposeToAssociations.foreach(dta => dta.writeToStandardFile())
    })
    createSurrogateBasedDtts(byLHS.values.toIndexedSeq)
    //TODO:       decomposedTemporalTable.writeToStandardFile()
  }


}
