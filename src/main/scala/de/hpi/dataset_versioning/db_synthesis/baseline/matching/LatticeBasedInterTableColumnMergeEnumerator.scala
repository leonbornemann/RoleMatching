package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.TimeIntervalSequence

case class LatticeBasedInterTableColumnMergeEnumerator() extends StrictLogging {

  logger.debug("Current implementation materializes all candidates - we can optimize this to iterate through it, but it is probably not worth it")
  logger.debug("We can also cut down on the memory requirements by combining <setToActiveTime> and <finalResultList>")

  def isValidMatch(existingSet: Set[AttributeLineage],
                   newAttributeCandidate: AttributeLineage,
                   setToActiveTime: collection.mutable.HashMap[Set[AttributeLineage], TimeIntervalSequence]): Boolean = {
    if (existingSet.contains(newAttributeCandidate))
      false
    else {
      val setActiveTime = setToActiveTime.getOrElseUpdate(existingSet, existingSet
        .map(_.activeTimeIntervals).
        reduce((a, b) => a.union(b)))
      newAttributeCandidate.activeTimeIntervals.intersect(setActiveTime).isEmpty
    }
  }

  def enumerateAll(inputSetAsList: IndexedSeq[AttributeLineage]) = {
    val setToActiveTime = collection.mutable.HashMap[Set[AttributeLineage], TimeIntervalSequence]()
    val finalResultList = collection.mutable.ArrayBuffer[Set[AttributeLineage]]() ++ inputSetAsList.map(Set(_))
    var done = false
    var prevLevel = collection.mutable.ArrayBuffer[Set[AttributeLineage]]() ++ finalResultList
    var nextLevel = collection.mutable.ArrayBuffer[Set[AttributeLineage]]()

    var curPrevLevelIndex = 0
    var curTestIndex = 1
    while (!done) {
      if (curPrevLevelIndex == prevLevel.size && nextLevel.isEmpty || curTestIndex > inputSetAsList.size) {
        done = true
      } else if (curPrevLevelIndex == prevLevel.size && !nextLevel.isEmpty) {
        prevLevel = nextLevel
        nextLevel = collection.mutable.ArrayBuffer[Set[AttributeLineage]]()
        curPrevLevelIndex = 0
      } else if (curTestIndex > inputSetAsList.size) {
        done = true
      } else {
        if (curTestIndex == inputSetAsList.size) {
          curPrevLevelIndex += 1
          curTestIndex = curPrevLevelIndex + 1
        } else {
          //we have an actual test to do!
          val existingSet = prevLevel(curPrevLevelIndex)
          val newAttributeCandidate = inputSetAsList(curTestIndex)
          if (isValidMatch(existingSet, newAttributeCandidate, setToActiveTime)) {
            val validCandidate = existingSet ++ Set(newAttributeCandidate)
            nextLevel += validCandidate
            finalResultList += validCandidate
          }
          curTestIndex += 1
        }
      }
    }
    finalResultList
  }
}
