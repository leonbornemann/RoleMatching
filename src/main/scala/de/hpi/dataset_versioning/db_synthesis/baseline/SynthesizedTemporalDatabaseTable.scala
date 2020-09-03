package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, EntityFieldLineage, ProjectedTemporalRow, TemporalColumn, TemporalRow, TemporalTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.{DataBasedMatchCalculator, PairwiseTupleMapper, TemporalDatabaseTableTrait, TemporalSchemaMapper}
import de.hpi.dataset_versioning.db_synthesis.baseline.index.ValueLineageIndex
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.{BinaryReadable, BinarySerializable, SynthesizedTemporalDatabaseTableSketch, TemporalColumnTrait, TemporalFieldTrait}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class SynthesizedTemporalDatabaseTable(val id:String,
                                       unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                       val schema: collection.IndexedSeq[AttributeLineage],
                                       val keyAttributeLineages: collection.Set[AttributeLineage],
                                       private val rows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer(),
                                       private var curEntityIDCounter:Long,
                                       val uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractTemporalDatabaseTable[Any](unionedTables) with StrictLogging with BinarySerializable{

  def numChanges = rows.map(tr => tr.fields.map(_.changeCount).sum).sum


  def writeToStandardTemporaryFile() = {
    val f = DBSynthesis_IOService.getSynthesizedTableTempFile(uniqueSynthTableID)
    writeToBinaryFile(f)
  }

  def writeToStandardFinalDatabaseFile() = {
    val f = DBSynthesis_IOService.getSynthesizedTableInFinalDatabaseFile(uniqueSynthTableID)
    writeToBinaryFile(f)
  }

  override def columns: IndexedSeq[TemporalColumnTrait[Any]] = {
    val a = (0 until schema.size).map(attrIndex => {
      val col = rows.map(tr => new EntityFieldLineage(tr.entityID,tr.fields(attrIndex)))
      new TemporalColumn(id,schema(attrIndex),col)
    })
    a
  }

  def getActiveTime = {
    schema.map(_.activeTimeIntervals).reduce((a,b) => a.union(b))
  }

  def nonKeyAttributeLineages = schema.filter(al => !keyAttributeLineages.contains(al))


  var keyIsArtificial = false

  private var fieldAndRowMappings = mutable.HashMap[DecomposedTemporalTable,FieldAndRowMapping]()

  def getBestMergeMapping(curCandidate: DecomposedTemporalTable) = {
    val greedyMatcher = new SuperSimpleGreedySchemaMatcher(this,curCandidate)
    greedyMatcher.getBestSchemaMatching()
  }

  override def primaryKey = keyAttributeLineages

  override def getID: String = id

  override def nrows: Int = columns.head.fieldLineages.size

  override def buildTemporalColumn(unionedColID: String,
                                   unionedAttrLineage: AttributeLineage,
                                   unionedFieldLineages: ArrayBuffer[TemporalFieldTrait[Any]],
                                   unionedTableID:String): TemporalColumnTrait[Any] = {
    new TemporalColumn(unionedColID,
      unionedAttrLineage,
      unionedFieldLineages.toIndexedSeq.zipWithIndex.map(t => EntityFieldLineage(t._2,ValueLineage(t._1.getValueLineage))))
  }

  override def buildNewTable(unionedTableID: String,
                             unionedTables: mutable.HashSet[DecomposedTemporalTableIdentifier],
                             pkIDSet: collection.Set[Int],
                             newTcSketches: Array[TemporalColumnTrait[Any]]): TemporalDatabaseTableTrait[Any] ={
    val newAttrsByID = newTcSketches.map(tcs => (tcs.attributeLineage.attrId,tcs.attributeLineage)).toMap
    val temporalRows = (0 until newTcSketches.head.fieldLineages.size).map(rID => {
      val fields = newTcSketches.map(tc => tc.fieldLineages(rID).asInstanceOf[ValueLineage])
      val newRow = new TemporalRow(rID,fields)
      newRow
    })
    new SynthesizedTemporalDatabaseTable(unionedTableID,
      unionedTables,
      newTcSketches.map(_.attributeLineage),
      pkIDSet.map(id => newAttrsByID(id)),
      mutable.ArrayBuffer() ++ temporalRows,
      0
    )
  }

  override def informativeTableName: String = getID + "(" + schema.map(_.lastName).mkString(",") + ")"
}
object SynthesizedTemporalDatabaseTable extends BinaryReadable[SynthesizedTemporalDatabaseTable] with StrictLogging {
  logger.debug("Potential Optimization: The columns method (in synth table), the columns have to be generated from the row representation - if this is too slow, it would be good to revisit this")

  def loadFromStandardFile(id:Int) = loadFromFile(DBSynthesis_IOService.getSynthesizedTableTempFile(id))

  def initFrom(dttToMerge: DecomposedTemporalTable) = {
    if(dttToMerge.id.viewID=="fullBaselineTest-A" && dttToMerge.id.bcnfID==1 && dttToMerge.id.associationID.isDefined && dttToMerge.id.associationID.get==1){
      println()
    }
    val synthesizedSchema = dttToMerge.containedAttrLineages
    var curEntityID:Long = 0
    val entityIDMatchingSynthesizedToOriginal = mutable.HashMap[Long,Long]()
    val tt = TemporalTable.loadAndCache(dttToMerge.id.viewID) //TODO: we will need to shrink this cache at some point
      .project(dttToMerge)
    val newRows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer()
    tt.projection.rows.foreach(tr => {
      val originalIds = mutable.HashMap[DecomposedTemporalTableIdentifier,Long](tt.projection.dttID.get.id -> tr.entityID)
      newRows.addOne(new SynthesizedTemporalRow(curEntityID,tr.fields,originalIds)) //TODO: use this
      entityIDMatchingSynthesizedToOriginal.put(curEntityID,tr.entityID)
      curEntityID +=1
    })
    val attributeMatchingSynthesizedToOriginal = synthesizedSchema.map(al => (al.attrId,al.attrId))
    val synthTable = new SynthesizedTemporalDatabaseTable(dttToMerge.compositeID,
      mutable.HashSet(dttToMerge.id),
      synthesizedSchema,
      dttToMerge.primaryKey,
      newRows,
      curEntityID)
    synthTable.fieldAndRowMappings.put(dttToMerge,new FieldAndRowMapping(attributeMatchingSynthesizedToOriginal,entityIDMatchingSynthesizedToOriginal))
    synthTable
  }

}