package de.hpi.dataset_versioning.io

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

object DBSynthesis_IOService extends StrictLogging{

  def getOptimizationInputAssociationSketchFile(head: DecomposedTemporalTableIdentifier) = {
    createParentDirs(new File(s"$OPTIMIZATION_INPUT_Association_SKETCH_DIR/${head.viewID}/${head.compositeID}.binary"))
  }

  def getOptimizationInputAssociationFile(head: DecomposedTemporalTableIdentifier) = {
    createParentDirs(new File(s"$OPTIMIZATION_INPUT_ASSOCIATION_DIR/${head.viewID}/${head.compositeID}.binary"))
  }

  def decomposedTemporalAssociationsExist(subdomain: String, id: String) = {
    val dir = getSurrogateBasedDecomposedTemporalAssociationDir(subdomain, id)
    dir.exists() && !dir.listFiles().isEmpty
  }

  def decomposedTemporalTablesExist(subdomain:String,id: String) = {
    val dir = getSurrogateBasedDecomposedTemporalTableDir(subdomain, id)
    dir.exists() && !dir.listFiles().isEmpty
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
    createParentDirs(new File(s"$DTT_SKETCH_DIR/${tableID.viewID}_${tableID.bcnfID}_${tableID.associationID.getOrElse("")}_$getVariantName.binary"))
  }

  def getTemporalColumnSketchDir(id: String) = new File(s"$COLUMN_SKETCH_DIR/$id/")


  def getTemporalColumnSketchFile(id: String, attrId: Int, fieldLineageSketchType:String) = createParentDirs(new File(s"$COLUMN_SKETCH_DIR/$id/${id}_${attrId}_$fieldLineageSketchType.binary"))


  def getStatisticsDir(subdomain: String, originalID: String) = createParentDirs(new File(s"$STATISTICS_DIR/$subdomain/$originalID/"))

  def getSurrogateBasedDecomposedTemporalTableDir(subdomain: String, viewID: String) = createParentDirs(new File(s"$DECOMPOSED_TEMPORAL_TABLE_DIR/$subdomain/$viewID/"))
  def getSurrogateBasedDecomposedTemporalAssociationDir(subdomain: String, viewID: String) = createParentDirs(new File(s"$DECOMPOSED_TEMPORAL_ASSOCIATION_DIR/$subdomain/$viewID/"))


  def getSurrogateBasedDecomposedTemporalTableFile(id:DecomposedTemporalTableIdentifier) = {
    val topDir = if(id.isPurePKToFKReference) DECOMPOSED_TEMPORAL_TABLE_PURE_PK_TO_FK_REFERENCES_DIR else if(id.associationID.isDefined) DECOMPOSED_TEMPORAL_ASSOCIATION_DIR else DECOMPOSED_TEMPORAL_TABLE_DIR
    createParentDirs(new File(s"$topDir/${id.subdomain}/${id.viewID}/${id.compositeID}.json"))
  }

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

  def DB_SYNTHESIS_DIR = socrataDir + "/db_synthesis"

  def DECOMPOSTION_DIR = DB_SYNTHESIS_DIR + "/decomposition"
  def STATISTICS_DIR = DB_SYNTHESIS_DIR + "/statistics/"

  def FDDIR = DECOMPOSTION_DIR + "/fds/"
  def COLID_FDDIR = DECOMPOSTION_DIR + "/fds_colID/"
  def DECOMPOSITION_EXPORT_CSV_DIR = DECOMPOSTION_DIR + "/csv/"
  def DECOMPOSITION_RESULT_DIR = DECOMPOSTION_DIR + "/results/"
  def DECOMPOSED_TABLE_DIR = DECOMPOSTION_DIR + "/decomposedTables/"
  def DECOMPOSED_TEMPORAL_TABLE_DIR = DECOMPOSTION_DIR + "/surrogateBasedDecomposedTemporalTables/"
  def DECOMPOSED_TEMPORAL_ASSOCIATION_DIR = DECOMPOSTION_DIR + "/surrogateBasedDecomposedTemporalAssociations/"
  def DECOMPOSED_TEMPORAL_TABLE_PURE_PK_TO_FK_REFERENCES_DIR = DECOMPOSTION_DIR + "/surrogateBasedDecomposedTemporalTablePKTPFK_REFERENCES/"
  def OPTIMIZATION_INPUT_Association_SKETCH_DIR = DB_SYNTHESIS_DIR + "/input/associationSketches/"
  def OPTIMIZATION_INPUT_ASSOCIATION_DIR = DB_SYNTHESIS_DIR + "/input/associations/"
  def SYNTHESIZED_TABLES_WORKING_DIR = DB_SYNTHESIS_DIR + "/workingDir/synthesizedTables/"
  def SYNTHESIZED_DATABASE_FINAL_DIR = DB_SYNTHESIS_DIR + "/finalSynthesizedDatabase/synthesizedTables/"
  def SKETCH_DIR = DB_SYNTHESIS_DIR + "/sketches/"
  def COLUMN_SKETCH_DIR = SKETCH_DIR + "/temporalColumns/"
  def DTT_SKETCH_DIR = SKETCH_DIR + "/decomposedTemporalTables/"

  def dateToStr(date: LocalDate) = IOService.dateTimeFormatter.format(date)

  def getDecompositionCSVExportFile(instance: DatasetInstance,subdomain:String) = {
    val dir = new File(DECOMPOSITION_EXPORT_CSV_DIR + "/" + subdomain + s"/${instance.id}/")
    dir.mkdirs()
    new File(dir.getAbsolutePath + s"/${dateToStr(instance.date)}.csv")
  }

  def getDecompositionResultFiles() = new File(DECOMPOSITION_RESULT_DIR).listFiles().filter(_.getName.endsWith(".json"))

  def socrataDir = IOService.socrataDir
}
