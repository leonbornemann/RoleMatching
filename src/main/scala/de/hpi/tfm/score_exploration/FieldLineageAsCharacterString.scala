package de.hpi.tfm.score_exploration

import de.hpi.tfm.data.socrata.change.ReservedChangeValues
import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, AttributeState, SurrogateAttributeLineage}
import de.hpi.tfm.data.socrata.simplified.Attribute
import de.hpi.tfm.data.tfmp_input.GlobalSurrogateRegistry
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.tfm.io.IOService

import java.time.LocalDate
import scala.collection.mutable

case class FieldLineageAsCharacterString(lineage: String, var label: String, rowNumber:Int = -1) {
  def dttID(subdomain:String): AssociationIdentifier = AssociationIdentifier.fromShortString(subdomain,label)

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
      val actualCurrent = if(c=='_') {
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

  def toValueLineage = {

    var prev:Option[Char] = None
    val asMap = lineage.zipWithIndex.map{case (c,i) => {
      var res:Option[(LocalDate,Any)] = None
      if(prev.isEmpty || prev.get != c) {
        val value:Any = if(c=='_') ReservedChangeValues.NOT_EXISTANT_COL else c
        res = Some((IOService.STANDARD_TIME_FRAME_START.plusDays(i),value))
      }
      prev = Some(c)
      res
    }}.filter(_.isDefined)
      .map(_.get)
      .toMap
    FactLineage(mutable.TreeMap[LocalDate,Any]() ++ asMap)
  }

  override def toString: String = s"$label:[" +lineage + "]"

}
object FieldLineageAsCharacterString{
  def mergeAll(values: Seq[FieldLineageAsCharacterString]) = {
    var totalRes = values(0)
    values.tail.foreach(v => totalRes = totalRes.mergeCompatible(v))
    FieldLineageAsCharacterString(totalRes.lineage,"M")
  }


  def createAttrs(fields: IndexedSeq[FieldLineageAsCharacterString], id: AssociationIdentifier) = {
    val id = GlobalSurrogateRegistry.getNextFreeSurrogateID
    val attr = new AttributeLineage(id,mutable.TreeMap(IOService.STANDARD_TIME_FRAME_START -> AttributeState(Some(Attribute(s"A_$id",id)))))
    val surrogateAttr = new SurrogateAttributeLineage(id,id)
    (attr,surrogateAttr)
  }

  def toAssociationTable(fields:IndexedSeq[FieldLineageAsCharacterString], id:AssociationIdentifier) = {
    //id:String,
    //                                                                unionedTables:mutable.HashSet[Int],
    //                                                                unionedOriginalTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
    //                                                                key: collection.IndexedSeq[SurrogateAttributeLineage],
    //                                                                nonKeyAttribute:AttributeLineage,
    //                                                                foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
    //                                                                val surrogateBasedTemporalRows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRow] = collection.mutable.ArrayBuffer(),
    val rows = fields.zipWithIndex.map{case (r,i) => new SurrogateBasedTemporalRow(IndexedSeq(i),r.toValueLineage,IndexedSeq())}
    val (attr,surrogateAttr) = createAttrs(fields,id)
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(id.compositeID,
      scala.collection.mutable.HashSet(id),
      IndexedSeq(surrogateAttr),
      attr,
      IndexedSeq(),
      mutable.ArrayBuffer() ++ rows
    )
  }
}
