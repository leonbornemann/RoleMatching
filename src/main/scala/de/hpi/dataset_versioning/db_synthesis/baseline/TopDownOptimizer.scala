package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.{MetadataBasedHeuristicMatchCalculator, SketchBasedHeuristicMatchCalculator}
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class TopDownOptimizer(associations: IndexedSeq[DecomposedTemporalTable]) extends StrictLogging{

  logger.debug("For now, we are taking the best heuristic match and unioning into this as long as we can - this should (maybe) be changed in the future")

  assert(associations.forall(_.isAssociation))

  private val dttByID = associations.map(a => (a.id,a)).toMap
  private val unmatchedAssociations = mutable.HashSet() ++ associations.map(dtt => SynthesizedTemporalDatabaseTableSketch.initFrom(dtt))
  private var curMatchedTable:SynthesizedTemporalDatabaseTable = null
  private val matchCandidateGraph = new MatchCandidateGraph(unmatchedAssociations,new SketchBasedHeuristicMatchCalculator())

  val MIN_TOP_MATCH_Score = 10 //TODO: this is arbitrary, we need to tune this!
  val MAX_NUM_TRIES_PER_ITERATION = 100

  def executeInitialMatch(bestMatch: TableUnionMatch) :(Option[SynthesizedTemporalDatabaseTable],SynthesizedTemporalDatabaseTableSketch) = {
    //load the actual table
    val sketchA = bestMatch.firstMatchPartner
    val sketchB = bestMatch.secondMatchPartner
    assert(sketchA.unionedTables.size == 1 && sketchB.unionedTables.size==1)
    val synthTableA = SynthesizedTemporalDatabaseTable.initFrom(dttByID(sketchA.unionedTables.head))
    val synthTableB = SynthesizedTemporalDatabaseTable.initFrom(dttByID(sketchB.unionedTables.head))
    synthTableA.tryUnion(synthTableB)
    ???
  }

  def findInitialMatch() = {
    while(curMatchedTable == null && !matchCandidateGraph.isEmpty) {
      val bestMatch = matchCandidateGraph.getNextBestHeuristicMatch()
      assert(bestMatch.isHeuristic)
      val (synthTable,synthTableSketch) = executeInitialMatch(bestMatch)
      if(synthTable.isDefined){
        curMatchedTable = synthTable.get
        matchCandidateGraph.updateGraphAfterMatchExecution(bestMatch,synthTable.get,synthTableSketch)
      } else{
        matchCandidateGraph.removeMatch(bestMatch)
      }
    }
  }

  def optimize() = {
    assert(curMatchedTable==null)
    findInitialMatch()

    while(!unmatchedAssociations.isEmpty){
      //explore a few of the heuristically promising candidates
      var numTries = 0
      while((matchCandidateGraph.noComputedMatchAvailable ||  matchCandidateGraph.getTopMatch().score < MIN_TOP_MATCH_Score)
        && numTries<MAX_NUM_TRIES_PER_ITERATION){
        matchCandidateGraph.getNextBestHeuristicMatch()
      }
    }
  }


}
