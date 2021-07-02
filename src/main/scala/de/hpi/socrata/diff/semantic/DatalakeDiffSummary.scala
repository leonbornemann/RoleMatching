package de.hpi.socrata.diff.semantic

import com.typesafe.scalalogging.StrictLogging

class DatalakeDiffSummary(var numInserts:Int = 0,
                         var numDeletes:Int = 0,
                         var numTupleChanges:Int = 0,
                         var numProjections:Int = 0,
                         var numColumnInserts:Int = 0,
                         var processed:Int = 0,
                         var numDataChanges:Int = 0,
                         var numDiffIncomplete:Int = 0) extends StrictLogging{

  def plus(other: DatalakeDiffSummary) = {
    numInserts +=other.numInserts
    numDeletes +=other.numDeletes
    numTupleChanges +=other.numTupleChanges
    numProjections +=other.numProjections
    numColumnInserts +=other.numColumnInserts
    processed +=other.processed
    numDataChanges +=other.numDataChanges
    numDiffIncomplete += other.numDiffIncomplete
  }

  def print() = {
    logger.trace(s"------------------------------------------------")
    logger.trace(s"processed: $processed")
    logger.trace(s"numInserts: $numInserts")
    logger.trace(s"numDeletes: $numDeletes")
    logger.trace(s"numTupleChanges: $numTupleChanges")
    logger.trace(s"projections: $numProjections")
    logger.trace(s"Column Inserts: $numColumnInserts")
    logger.trace(s"num Incomplete Diffs: $numDiffIncomplete")
    logger.trace(s"num Revisions with at least one change: $numDataChanges")
    logger.trace(s"------------------------------------------------")
  }

}
