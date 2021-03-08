package de.hpi.dataset_versioning.io

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

import java.io.File
import java.time.LocalDate
import scala.reflect.io.Directory

object DBSynthesis_IOService extends StrictLogging{

  def DB_SYNTHESIS_DIR = socrataDir + "/db_synthesis"

  //statistics and reporting:
  def STATISTICS_DIR = DB_SYNTHESIS_DIR + "/statistics/"
  def WORKING_DIR = DB_SYNTHESIS_DIR + "/workingDir/"
  //decomposition:
  def DECOMPOSTION_DIR = DB_SYNTHESIS_DIR + "/decomposition"
  def FDDIR = DECOMPOSTION_DIR + "/fds/"
  def COLID_FDDIR = DECOMPOSTION_DIR + "/fds_colID/"
  def DECOMPOSITION_EXPORT_CSV_DIR = DECOMPOSTION_DIR + "/csv/"
  def DECOMPOSITION_RESULT_DIR = DECOMPOSTION_DIR + "/results/"
  def DECOMPOSED_TABLE_DIR = DECOMPOSTION_DIR + "/decomposedTables/"
  def DECOMPOSED_Temporal_TABLE_DIR = DECOMPOSTION_DIR + "/decomposedTemporalTables/"
  def BCNF_SCHEMA_FILE = DECOMPOSTION_DIR + "/bcnfSchemata/"
  //association schema and input
  def ASSOCIATION_SCHEMA_DIR = DECOMPOSTION_DIR + "/associationSchemata/"
  def OPTIMIZATION_INPUT_DIR = DB_SYNTHESIS_DIR + "/input/"
  def OPTIMIZATION_INPUT_ASSOCIATION_SKETCH_DIR = OPTIMIZATION_INPUT_DIR + "/associationSketches/"
  def OPTIMIZATION_INPUT_ASSOCIATION_DIR = OPTIMIZATION_INPUT_DIR + "/associations/"
  def OPTIMIZATION_INPUT_BCNF_DIR = OPTIMIZATION_INPUT_DIR + "/BCNF/"
  //database generation output
  def DATABASE_ROOT_DIR = DB_SYNTHESIS_DIR + "/synthesizedDatabases/"
  def SYNTHESIZED_TABLES_WORKING_DIR = DB_SYNTHESIS_DIR + "/workingDir/synthesizedTables/"
  def SYNTHESIZED_DATABASE_FINAL_DIR = DB_SYNTHESIS_DIR + "/finalSynthesizedDatabase/synthesizedTables/"
  def SKETCH_DIR = DB_SYNTHESIS_DIR + "/sketches/"
  def COLUMN_SKETCH_DIR = SKETCH_DIR + "/temporalColumns/"
  //mergeability graphs:
  def ASSOCIATIONS_MERGEABILITY_GRAPH_DIR = OPTIMIZATION_INPUT_DIR + "/associationMergeabilityGraphs/"
  def ASSOCIATIONS_MERGEABILITY_SINGLE_EDGE_DIR = ASSOCIATIONS_MERGEABILITY_GRAPH_DIR + "/singleEdgeFiles/"
  def FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR = OPTIMIZATION_INPUT_DIR + "/fieldLineageMergeabilityGraph/"
  def CONNECTED_COMPONENT_DIR(subdomain:String) = createParentDirs(new File(OPTIMIZATION_INPUT_DIR + s"/connectedComponents/$subdomain")).getAbsolutePath
  def CONNECTED_COMPONENT_FILE(subdomain:String,filecounter:Int) = createParentDirs(new File(OPTIMIZATION_INPUT_DIR + s"/connectedComponents/$subdomain/$filecounter.txt")).getAbsolutePath

  def FIELD_MERGE_RESULT_DIR = OPTIMIZATION_INPUT_DIR + "/mergedTuples/"

  def getAssociationGraphEdgeCandidateFile = new File(WORKING_DIR + "associationGraphEdgeCandidates.json")

  def getAssociationGraphEdgeFile = createParentDirs(new File(WORKING_DIR + "associationGraphEdges.json"))

  def getAssociationsWithChangesFile() = IOService.CUSTOM_METADATA_DIR + "associationsWithChanges.json"

  def clearDatabaseSynthesisInputDir() = new Directory(new File(OPTIMIZATION_INPUT_DIR)).deleteRecursively()


  def getSurrogateBasedDecomposedTemporalTableFile(id: DecomposedTemporalTableIdentifier) = {
    createParentDirs(new File(s"$DECOMPOSED_Temporal_TABLE_DIR/${id.viewID}/${id.compositeID}.json"))
  }

  def getSurrogateBasedDecomposedTemporalTableDir(subdomain: String, originalID: String) = {
    createParentDirs(new File(s"$DECOMPOSED_Temporal_TABLE_DIR/${originalID}/"))
  }

  def getOptimizationBCNFReferenceTableInputFile(id: DecomposedTemporalTableIdentifier) = {
    createParentDirs(new File(s"$OPTIMIZATION_INPUT_BCNF_DIR/referenceTables/${id.viewID}/${id.compositeID}.json"))
  }

  def getOptimizationBCNFTemporalTableFile(id: DecomposedTemporalTableIdentifier) = {
    createParentDirs(new File(s"$OPTIMIZATION_INPUT_BCNF_DIR/contentTables/${id.viewID}/${id.compositeID}.binary"))
  }

  //clear working directory for synthesized tables:
  logger.debug("Beginning to delete old synthesized tables from working directory")
  if(new File(SYNTHESIZED_TABLES_WORKING_DIR).exists()){
    new File(SYNTHESIZED_TABLES_WORKING_DIR).listFiles().foreach(f => f.delete())
  }
  logger.debug("done")

  def getSynthesizedTableInFinalDatabaseFile(uniqueSynthTableID: Int) = createParentDirs(new File(s"$SYNTHESIZED_DATABASE_FINAL_DIR/$uniqueSynthTableID.binary"))
  def getSynthesizedTableTempFile(uniqueSynthTableID: Int) = createParentDirs(new File(SYNTHESIZED_TABLES_WORKING_DIR + s"/$uniqueSynthTableID.binary"))


  def getDecomposedTemporalTableSketchFile(tableID: DecomposedTemporalTableIdentifier, getVariantName: String) = {
    ???
    //createParentDirs(new File(s"$DTT_SKETCH_DIR/${tableID.viewID}_${tableID.bcnfID}_${tableID.associationID.getOrElse("")}_$getVariantName.binary"))
  }

  def getTemporalColumnSketchDir(id: String) = new File(s"$COLUMN_SKETCH_DIR/$id/")


  def getTemporalColumnSketchFile(id: String, attrId: Int, fieldLineageSketchType:String) = createParentDirs(new File(s"$COLUMN_SKETCH_DIR/$id/${id}_${attrId}_$fieldLineageSketchType.binary"))


  def getStatisticsDir(subdomain: String, originalID: String) = createParentDirs(new File(s"$STATISTICS_DIR/$subdomain/$originalID/"))

  def getColIDFDFile(subdomain: String, id: String, date: LocalDate) = {
    createParentDirs(new File(s"$COLID_FDDIR/$subdomain/$id/${IOService.dateTimeFormatter.format(date)}.json"))
  }

  def createParentDirs(f:File) = {
    val parent = f.getParentFile
    parent.mkdirs()
    f
  }

  def getDecomposedTableFile(subdomain:String,id: String,date:LocalDate) =
    createParentDirs(new File(s"$DECOMPOSED_TABLE_DIR/$subdomain/$id/${IOService.dateTimeFormatter.format(date)}.json"))
  def getFDFile(subdomain:String,id: String, date: LocalDate) =
    createParentDirs(new File(s"$FDDIR/$subdomain/$id/${IOService.dateTimeFormatter.format(date)}.csv-hyfd.txt"))
  def getExportedCSVFile(subdomain:String,id: String, date: LocalDate) =
    createParentDirs(new File(s"$DECOMPOSITION_EXPORT_CSV_DIR/$subdomain/$id/${IOService.dateTimeFormatter.format(date)}.csv"))

  def getExportedCSVSubdomainDir(subdomain:String) =
    createParentDirs(new File(s"$DECOMPOSITION_EXPORT_CSV_DIR/$subdomain/"))
  def getDecomposedTablesDir(subdomain:String) =
    createParentDirs(new File(s"$DECOMPOSED_TABLE_DIR/$subdomain/"))

  def getSortedFDFiles(subdomain:String,id: String) = new File(s"$FDDIR/$subdomain/$id/")
    .listFiles()
    .toIndexedSeq
    .sortBy(f => LocalDate.parse(f.getName.split("\\.")(0),IOService.dateTimeFormatter).toEpochDay)

  def dateToStr(date: LocalDate) = IOService.dateTimeFormatter.format(date)

  def getDecompositionCSVExportFile(instance: DatasetInstance,subdomain:String) = {
    val dir = new File(DECOMPOSITION_EXPORT_CSV_DIR + "/" + subdomain + s"/${instance.id}/")
    dir.mkdirs()
    new File(dir.getAbsolutePath + s"/${dateToStr(instance.date)}.csv")
  }

  def getDecompositionResultFiles() = new File(DECOMPOSITION_RESULT_DIR).listFiles().filter(_.getName.endsWith(".json"))

  def socrataDir = IOService.socrataDir
}
