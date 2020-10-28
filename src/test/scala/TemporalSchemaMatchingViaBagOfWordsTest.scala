import java.time.LocalDate

import FullBaselinePipelineTest.{idA, idB, ttA, versions}
import de.hpi.dataset_versioning.data.change.ChangeExporter
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SynthesizedTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TemporalSchemaMapper
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object TemporalSchemaMatchingViaBagOfWordsTest extends App {
  IOService.socrataDir = "/home/leon/data/dataset_versioning/socrata/testDir/"
  val versions = IndexedSeq(LocalDate.parse("2019-11-01"))
  val idA = "temporalSchemaMapping_BagOfWordsTest-1"
  val idB = "temporalSchemaMapping_BagOfWordsTest-2"
  val exporter = new ChangeExporter
  val allViewIds = Seq(idA,idB)
  allViewIds.foreach(exporter.exportAllChangesFromVersions(_,versions))
  val ttA = TemporalTable.load(idA)
  val ttB = TemporalTable.load(idB)
  val pkA1 = ttA.attributes.filter(al => Seq(0,1,2).contains(al.attrId)).toSet
  val pkB1 = ttB.attributes.filter(al => Seq(0,1,2).contains(al.attrId)).toSet
  //associations
  val dttA = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttA.id, 0, Some(0)),
    mutable.ArrayBuffer() ++ ttA.attributes.filter(_.attrId!=4),
    pkA1,
    Map(versions(0)-> pkA1.map(_.valueAt(versions(0))._2.attr.get)),
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  val dttB = new DecomposedTemporalTable(DecomposedTemporalTableIdentifier("test-subdomain", ttB.id, 0, Some(0)),
    mutable.ArrayBuffer() ++ ttB.attributes.filter(_.attrId!=4),
    pkB1,
    Map(versions(0)-> pkB1.map(_.valueAt(versions(0))._2.attr.get)),
    mutable.HashSet[DecomposedTemporalTableIdentifier]()
  )
  dttA.writeToStandardFile()
  dttB.writeToStandardFile()
  val schemaMapper = new TemporalSchemaMapper()
  val tableA = SynthesizedTemporalDatabaseTable.initFrom(dttA,ttA)
  val tableB = SynthesizedTemporalDatabaseTable.initFrom(dttB,ttB)
  val res = schemaMapper.enumerateAllValidSchemaMappings(tableA,tableB)
  assert(res.size==1)
  assert(res.head.forall(t => t._1.head.nameSet == t._2.head.nameSet))
  println(res)
}
