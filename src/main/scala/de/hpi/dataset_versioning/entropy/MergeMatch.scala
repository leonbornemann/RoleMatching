package de.hpi.dataset_versioning.entropy

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.util.MathUtil.log2

import java.io.PrintWriter

case class MergeMatch(first: FieldLineageAsCharacterString, second: FieldLineageAsCharacterString) {

  val subdomain = "org.cityofchicago"

  val merged = first.mergeCompatible(second)
  val firstEntropy = first.defaultEntropy
  val secondEntropy = second.defaultEntropy
  val mergedEntropy = merged.defaultEntropy

  def mutualInformationWCEQUALTOWC = {
    //variant with WC = WC
    val transitions1 = first.getTransitions(first.lineage,false,true)
    val transitions2 = second.getTransitions(second.lineage,false,true)
    val togetherAtSameTime = scala.collection.mutable.HashMap[((Char,Char),(Char,Char)),Int]()
    val denominator = first.lineage.length - 1
    (1 until first.lineage.length).foreach(i => {
      val transition1 = (first.lineage(i-1),first.lineage(i))
      val transition2 = (second.lineage(i-1),second.lineage(i))
      val key = (transition1, transition2)
      val prev = togetherAtSameTime.getOrElse(key,0)
      togetherAtSameTime(key) = prev+1
    })
    var nominatorSum = 0.0
    val mutualInfoTerms = togetherAtSameTime.map{case ((x,y),jointMass) => {
      val jointMassProbability = jointMass / denominator.toDouble
      val marginalX = transitions1(x) / denominator.toDouble
      val marginalY = transitions2(y) / denominator.toDouble
      nominatorSum += jointMassProbability
      val logTerm = log2(jointMassProbability / (marginalX * marginalY))
      val result = jointMassProbability * logTerm
      result
    }}
    if(nominatorSum<=0.99999 || nominatorSum >=1.000001){
      println()
    }
    assert(!(nominatorSum<=0.99999 || nominatorSum >=1.000001))
    mutualInfoTerms.sum
  }

  def mutualInformationWCNOTEQUALWC = {
    MergeMatch.mutualInformationWCNOTEQUALWC(first,second)
  }

  def entropyDifferenceBeweenOriginals = Math.abs(first.defaultEntropy - second.defaultEntropy)

  def makeLineageString(first: String) = "[" + first.mkString(",") + "]"

  // DIFFERENCE:${entropyDifferenceBeweenOriginals}%1.3f
  def printShort = {
    println(f"DIFF: $entropyDifferenceBeweenOriginals%1.3f SCORE:$scoreFunction%1.3f MERGED:$mergedEntropy%1.3f")
    first.printWithEntropy
    second.printWithEntropy
    println()
  } //(${entropy(elem)}%1.3f) MERGE  $s (${entropy(s)}%1.3f) TO $merged (${entropy(merged)}%1.3f)")

  def entropyReduction = firstEntropy + secondEntropy - mergedEntropy

  def scoreFunction = mutualInformationWCNOTEQUALWC//entropyReduction

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
    val evidence = tuple1.getOverlapEvidenceCount(tuple2)
    println()
  }
}

object MergeMatch {

  def mutualInformationWCNOTEQUALWC(first: FieldLineageAsCharacterString, second: FieldLineageAsCharacterString) = {
    val transitionsX = first.getTransitions(first.lineage,false,true)
    val transitionsY = second.getTransitions(second.lineage,false,true)
    val togetherAtSameTime = scala.collection.mutable.HashMap[((Char,Char),(Char,Char)),Int]()
    (1 until first.lineage.length).foreach(i => {
      val transition1 = (first.lineage(i-1),first.lineage(i))
      val transition2 = (second.lineage(i-1),second.lineage(i))
      val key = (transition1, transition2)
      val prev = togetherAtSameTime.getOrElse(key,0)
      togetherAtSameTime(key) = prev+1
    })
    var nominatorSum = 0.0
    var mutualInfo = 0.0
    val processed = scala.collection.mutable.HashSet[((Char,Char),(Char,Char))]()
    val denominator = first.lineage.length - 1
    (1 until first.lineage.length).foreach(i => {
      val x = (first.lineage(i-1),first.lineage(i))
      val y = (second.lineage(i-1),second.lineage(i))
      val key = (x, y)
      if(!processed.contains(key)){
        if(Seq(x._1,x._2,y._1,y._2).exists(_=='_')){
          //joint mass is 1/size, so are the individuals
          val jointMassProbability = 1 / denominator.toDouble
          val marginalX = if(x._1=='_' || x._2=='_') 1 / denominator.toDouble else transitionsX(x) / denominator.toDouble
          val marginalY = if(y._1=='_' || y._2=='_') 1 / denominator.toDouble else transitionsY(y) / denominator.toDouble
          val logTerm = log2(jointMassProbability / (marginalX * marginalY))
          val result = jointMassProbability * logTerm
          nominatorSum += jointMassProbability
          mutualInfo += result
        } else {
          val jointMass = togetherAtSameTime(key)
          val jointMassProbability = jointMass / denominator.toDouble
          val marginalX = transitionsX(x) / denominator.toDouble
          val marginalY = transitionsY(y) / denominator.toDouble
          val logTerm = log2(jointMassProbability / (marginalX * marginalY))
          val result = jointMassProbability * logTerm
          nominatorSum += jointMassProbability
          mutualInfo+=result
          processed.add(key)
        }
      }
    })
    if(nominatorSum<=0.99999 || nominatorSum >=1.000001){
      println()
    }
    assert(!(nominatorSum<=0.99999 || nominatorSum >=1.000001))
    mutualInfo
  }

}
