package de.hpi.dataset_versioning.entropy

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

import scala.collection.mutable

case class FieldLineageAsCharacterString(lineage: String, label: String, rowNumber:Int = -1) {
  def dttID(subdomain:String): DecomposedTemporalTableIdentifier = DecomposedTemporalTableIdentifier.fromShortString(subdomain,label)

  def printWithEntropy = println(toString + f" ($defaultEntropy%1.3f)")

  def defaultEntropy = entropyV7

  def mergeCompatible(other: FieldLineageAsCharacterString) = {
    if (lineage.size != other.lineage.size)
      throw new AssertionError("not same size")
    val s1 = lineage
    val s2 = other.lineage
    val newSequence = (0 until s1.size).map(i => {
      if (s1(i) == s2(i)) s1(i)
      else if (s1(i) == '_') s2(i)
      else if (s2(i) == '_') s1(i)
      else throw new AssertionError(s"not compatible at index ${i}")
    })
    FieldLineageAsCharacterString(newSequence.mkString, label + "&" + other.label)
  }

  //DEPRECATED:
  //    def entropyV1:Double = {
  //      entropyV1(getTransitions(lineage))
  //    }
  //
  //    def entropyV1(transitions: mutable.TreeMap[(Char, Char), Int]):Double = {
  //      - transitions.values.map(count => {
  //        val pXI = count / transitions.values.sum.toDouble
  //        pXI * log2(pXI)
  //      }).sum
  //    }

  def entropyV4: Double = {
    entropyV2(getTransitions(lineage) ++ getTransitions(lineage.reverse), lineage.length)
  }

  def entropyV5 = {
    entropyV2(getTransitions(lineage, true) ++ getTransitions(lineage.reverse, true), lineage.length)
  }

  def entropyV6 = {
    entropyV2(getTransitions(lineage,false,true),lineage.length)
  }

  def entropyV7 = {
    entropyV2(getTransitionsWildCardUnequalWildcard(lineage),lineage.length)
  }

  def entropyV3: Double = {
    entropyV2(getTransitions(lineage, true), lineage.length)
  }

  def entropyV2: Double = {
    entropyV2(getTransitions(lineage), lineage.length)
  }

  def entropyV2(transitions: mutable.HashMap[Any, Int], lineageSize: Int): Double = {
    -transitions.values.map(count => {
      val pXI = count / (lineageSize - 1).toDouble
      pXI * log2(pXI)
    }).sum
  }

  def log2(a: Double) = math.log(a) / math.log(2)

  def getTransitionsWildCardUnequalWildcard(finalString:String) = {
    var prev = finalString(0)
    val transitions = mutable.HashMap[Any, Int]()
    var curWCCount = 0
    finalString.tail.foreach(c => {
      val actualPrev = if(prev=='_') {
        curWCCount+=1
        s"WC_$curWCCount"
      } else prev.toString
      val actualCurrent = if(prev=='_') {
        curWCCount+=1
        s"WC_$curWCCount"
      } else c.toString
      val prevCount = transitions.getOrElseUpdate((actualPrev, actualCurrent), 0)
      transitions((actualPrev, actualCurrent)) = prevCount + 1
      prev = c
    })
    transitions
  }

  def getTransitions(stringWithWildcards: String,
                     countOnlyTrueChange: Boolean = false,
                     countWildcardsNormally:Boolean=false) = {
    val finalString = if(!countWildcardsNormally) stringWithWildcards.filter(_ != '_') else stringWithWildcards
    var prev = finalString(0)
    val transitions = mutable.HashMap[Any, Int]()
    finalString.tail.foreach(c => {
      if (!countOnlyTrueChange || c != prev) {
        val prevCount = transitions.getOrElseUpdate((prev, c), 0)
        transitions((prev, c)) = prevCount + 1
        prev = c
      }
    })
    transitions
  }

  override def toString: String = s"$label:[" +lineage + "]"

}
