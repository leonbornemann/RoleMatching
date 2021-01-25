package de.hpi.dataset_versioning.entropy

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation

import java.io.PrintWriter

case class MergeMatch(first: FieldLineageAsCharacterString, second: FieldLineageAsCharacterString) {

  val subdomain = "org.cityofchicago"

  val merged = first.mergeCompatible(second)
  val firstEntropy = first.defaultEntropy
  val secondEntropy = second.defaultEntropy
  val mergedEntropy = merged.defaultEntropy


  def entropyDifferenceBeweenOriginals = Math.abs(first.defaultEntropy - second.defaultEntropy)

  def makeLineageString(first: String) = "[" + first.mkString(",") + "]"

  // DIFFERENCE:${entropyDifferenceBeweenOriginals}%1.3f
  def printShort = {
    println(f"DIFF: $entropyDifferenceBeweenOriginals%1.3f REDUCTION:$entropyReduction%1.3f MERGED:$mergedEntropy%1.3f")
    first.printWithEntropy
    second.printWithEntropy
    println()
  } //(${entropy(elem)}%1.3f) MERGE  $s (${entropy(s)}%1.3f) TO $merged (${entropy(merged)}%1.3f)")

  def entropyReduction = firstEntropy + secondEntropy - mergedEntropy

  def exportActualTableMatch(targetPath:String) = {
    val pr = new PrintWriter(targetPath)
    new AssociationGraphExplorer().printInfoToFile(None,first.dttID(subdomain),second.dttID(subdomain),pr)
  }

  def printActualTuples = {
    val t1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(first.dttID(subdomain))
    val t2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(second.dttID(subdomain))
    val tuple1 = t1.rows(first.rowNumber).valueLineage
    val tuple2 = t2.rows(second.rowNumber).valueLineage
    printShort
    println(tuple1.lineage)
    println(tuple2.lineage)
    val a = tuple1.countChanges(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD)
    val b = tuple2.countChanges(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD)
    val c = tuple1.mergeWithConsistent(tuple2).countChanges(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD)
    val evidence = tuple1.countOverlapEvidence(tuple2)
    println()
  }
}
