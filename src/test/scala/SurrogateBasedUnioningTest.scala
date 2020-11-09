import java.io.File
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, AttributeState, SurrogateAttributeLineage, TemporalRow, TemporalTable}
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.TopDown
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.index.{LayeredTupleIndex, MostDistinctTimestampIndexBuilder}
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import scala.collection.mutable
import scala.io.Source
import scala.reflect.io.Directory

object SurrogateBasedUnioningTest extends App {

  val subdomain = "subdomain"

  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
  DBSynthesis_IOService.clearDatabaseSynthesisInputDir()
  new Directory(new File(DBSynthesis_IOService.DECOMPOSTION_DIR)).deleteRecursively() //we don't even want a method for this is this is truly danger-zone

  val testPaths = new File("/home/leon/data/dataset_versioning/socrata/testDir/TestDataShortNotation/")
    .listFiles()

  def parseAndSerializeTestData(testPath: File) = {
    val lines = Source.fromFile(testPath).getLines().toIndexedSeq
    var nextAttrID=0
    val keyCols = lines(0)
      .split(":")(1)
      .split(",")
      .map(_.toInt)
    val bcnfSurrogateKey = keyCols.map(i => SurrogateAttributeLineage(GlobalSurrogateRegistry.getNextFreeSurrogateID,i,intToTime(0)))
    val schema = lines(1).split(",")
      .map(elem => {
        val thisID = nextAttrID
        nextAttrID+=1
        val attr = Attribute(elem, thisID, Some(thisID), None)
        val originalAttr = new AttributeLineage(thisID,mutable.TreeMap(intToTime(0) -> AttributeState(Some(attr))))
        val surrogate = SurrogateAttributeLineage(GlobalSurrogateRegistry.getNextFreeSurrogateID,thisID,intToTime(0))
        (originalAttr,surrogate)
      })
    val viewID = testPath.getName
    val id = DecomposedTemporalTableIdentifier(subdomain,viewID,0,None)
    val bcnfSchema = new BCNFTableSchema(id,bcnfSurrogateKey,schema.map(_._2),IndexedSeq())
    val rows = lines
      .slice(2,lines.size)
      .zipWithIndex
      .map{case (s,rowIndex) => {
        val lineages = s.split(",").map(fieldString => {
          assert(fieldString.head=='[' && fieldString.last==']')
          val lineage = mutable.TreeMap[LocalDate,Any]() ++ fieldString.substring(1,fieldString.length-1)
            .split(";")
            .map(lineageElemString => {
              val time = lineageElemString.split(":")(0).toInt
              val elem = lineageElemString.split(":")(1)
              (intToTime(time),elem)
            })
          ValueLineage(lineage)
        })
        new TemporalRow(rowIndex,lineages)
      }}
    val associations = schema.zipWithIndex.map{case (rhs,associationTableID) => new AssociationSchema(DecomposedTemporalTableIdentifier(subdomain,viewID,0,Some(associationTableID)),
      rhs._2,
      rhs._1)}
    val tt = new TemporalTable(viewID,schema.map(_._1),rows,None)
    tt.addSurrogates(bcnfSchema.surrogateKey.toSet ++ associations.map(_.surrogateKey).toSet)
    val (bcnf,associationTables) = tt.project(bcnfSchema,associations)
    //write to standard files
    bcnfSchema.writeToStandardFile()
    //write temporal schema (of view):
    val ts = new TemporalSchema(viewID,schema.map(_._1))
    ts.writeToStandardFile()
    //write association schema
    associations.foreach(_.writeToStandardFile())
    //write content:
    bcnf.writeToStandardOptimizationInputFile
    associationTables.foreach(ref => {
      ref.writeToStandardOptimizationInputFile
      ref.toSketch.writeToStandardOptimizationInputFile()
    })
    (viewID,associationTables)
  }

  def intToTime(t:Int) = LocalDate.of(2019,11,1).plusDays(t)


  val (ids) = testPaths
    .map(tp => parseAndSerializeTestData(tp))
    .toIndexedSeq
  val tables = ids.map(_._2).flatten
  val viewIDs = ids.map(_._1)
  val goalsTable = tables.filter(a => a.toString.contains("goal-s000") && a.toString.contains("Team")).head
  val teamTable = tables.filter(a => a.toString.contains("team-s000") && a.toString.contains("Team")).head
  println()
  val indexBuilder = new MostDistinctTimestampIndexBuilder[Any](Set(goalsTable,teamTable),true)
  val index = indexBuilder.buildTableIndexOnNonKeyColumns()
  val groups = index.tupleGroupIterator.toIndexedSeq
  val topDown = new TopDown(subdomain)
  topDown.synthesizeDatabase(viewIDs,false)

}
