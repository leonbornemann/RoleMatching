package de.hpi.tfm.io

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.socrata.DatasetInstance
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier

import java.io.File
import java.time.LocalDate
import scala.reflect.io.Directory

object DBSynthesis_IOService extends StrictLogging{
  def DB_SYNTHESIS_DIR = socrataDir + "/db_synthesis"

  //statistics and reporting:
  def STATISTICS_DIR = DB_SYNTHESIS_DIR + "/statistics/"
  def WORKING_DIR = DB_SYNTHESIS_DIR + "/workingDir/"
  //decomposition:
  def DECOMPOSTION_DIR(subdomain:String) = DB_SYNTHESIS_DIR + s"/decomposition/$subdomain/"
  def FDDIR(subdomain:String) = DECOMPOSTION_DIR(subdomain) + "/fds/"
  def COLID_FDDIR(subdomain:String) = DECOMPOSTION_DIR(subdomain) + "/fds_colID/"
  def BCNF_SCHEMA_FILE(subdomain:String) = DECOMPOSTION_DIR(subdomain) + "/bcnfSchemata/"
  //association schema and input
  def ASSOCIATION_SCHEMA_DIR(subdomain:String) = DECOMPOSTION_DIR(subdomain) + "/associationSchemata/"
  def OPTIMIZATION_INPUT_DIR(subdomain:String) = DB_SYNTHESIS_DIR + s"/input/$subdomain/"
  def OPTIMIZATION_INPUT_ASSOCIATION_SKETCH_DIR(subdomain:String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/associationSketches/"
  def OPTIMIZATION_INPUT_ASSOCIATION_DIR(subdomain:String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/associations/"
  def OPTIMIZATION_INPUT_BCNF_DIR(subdomain:String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/BCNF/"
  def OPTIMIZATION_INPUT_FULL_TIME_RANGE_ASSOCIATION_DIR(subdomain:String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/FullTimeRangeAssociations/"
  def OPTIMIZATION_INPUT_FULL_TIME_RANGE_ASSOCIATION_SKETCH_DIR(subdomain: String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/FullTimeRangeAssociationSketches/"
  def OPTIMIZATION_INPUT_FACTLOOKUP_DIR(viewID:String,subdomain:String) = OPTIMIZATION_INPUT_DIR(subdomain) + s"/factLookupTables/$viewID/"
  //mergeability graphs:
  def ASSOCIATIONS_MERGEABILITY_GRAPH_DIR(subdomain:String) = OPTIMIZATION_INPUT_DIR(subdomain) + s"/associationMergeabilityGraphs/"
  def ASSOCIATIONS_MERGEABILITY_SINGLE_EDGE_DIR(subdomain:String) = ASSOCIATIONS_MERGEABILITY_GRAPH_DIR(subdomain) + s"/singleEdgeFiles/"
  def FIELD_LINEAGE_MERGEABILITY_GRAPH_DIR(subdomain:String) = OPTIMIZATION_INPUT_DIR(subdomain) + s"/fieldLineageMergeabilityGraph/"
  def COMPATIBILITY_GRAPH_DIR(subdomain:String) = OPTIMIZATION_INPUT_DIR(subdomain) + "/compatibilityGraphs/"
  def CONNECTED_COMPONENT_DIR(subdomain:String) = createParentDirs(new File(OPTIMIZATION_INPUT_DIR(subdomain) + s"/connectedComponents/")).getAbsolutePath
  def CONNECTED_COMPONENT_FILE(subdomain:String,filecounter:Int) = createParentDirs(new File(OPTIMIZATION_INPUT_DIR(subdomain) + s"/connectedComponents/$filecounter.txt")).getAbsolutePath
  def FIELD_MERGE_RESULT_DIR(subdomain:String,methodName: String) = createParentDirs(new File(OPTIMIZATION_INPUT_DIR(subdomain) + s"/mergedTuples/$methodName/")).getAbsolutePath

  //EVALUATION:
  def EVALUATION_DIR(subdomain:String) = createParentDirs(new File(DB_SYNTHESIS_DIR + s"/evaluationResults/$subdomain/")).getAbsolutePath
  def EVALUATION_RESULT_DIR(subdomain:String,methodName: String) = createParentDirs(new File(EVALUATION_DIR(subdomain) + s"/$methodName/")).getAbsolutePath

  def getAssociationGraphEdgeCandidateFile(subdomain:String,graphConfig:GraphConfig) =
    createParentDirs(new File(OPTIMIZATION_INPUT_DIR(subdomain) + s"/associationMergeabilityGraphCandidates/${graphConfig.minEvidence}_${dateToStr(graphConfig.timeRangeStart)}_${dateToStr(graphConfig.timeRangeEnd)}.json"))
  def getAssociationGraphEdgeCandidatePartitionDir(subdomain: String,graphConfig: GraphConfig) =
    createParentDirs(new File(OPTIMIZATION_INPUT_DIR(subdomain) + s"/associationMergeabilityGraphCandidates/partitions/${graphConfig.minEvidence}_${dateToStr(graphConfig.timeRangeStart)}_${dateToStr(graphConfig.timeRangeEnd)}/"))
  def getAssociationGraphEdgeCandidatePartitionFile(subdomain: String,graphConfig: GraphConfig, curPartitionNum: Int) =
    createParentDirs(new File(getAssociationGraphEdgeCandidatePartitionDir(subdomain,graphConfig).getAbsolutePath + s"/$curPartitionNum.json"))

  def getOptimizationBCNFReferenceTableInputFile(id: AssociationIdentifier) = {
    createParentDirs(new File(s"${OPTIMIZATION_INPUT_BCNF_DIR(id.subdomain)}/referenceTables/${id.viewID}/${id.compositeID}.json"))
  }

  def getOptimizationBCNFTemporalTableFile(id: AssociationIdentifier) = {
    createParentDirs(new File(s"${OPTIMIZATION_INPUT_BCNF_DIR(id.subdomain)}/contentTables/${id.viewID}/${id.compositeID}.binary"))
  }

  def getStatisticsDir(subdomain: String, originalID: String) = createParentDirs(new File(s"$STATISTICS_DIR/$subdomain/$originalID/"))

  def createParentDirs(f:File) = {
    val parent = f.getParentFile
    parent.mkdirs()
    f
  }

  def dateToStr(date: LocalDate) = IOService.dateTimeFormatter.format(date)

  def socrataDir = IOService.socrataDir
}
