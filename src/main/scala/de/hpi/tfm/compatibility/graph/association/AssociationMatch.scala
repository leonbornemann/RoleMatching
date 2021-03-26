package de.hpi.tfm.compatibility.graph.association

import de.hpi.tfm.compatibility.graph.fact.TupleSetMatching
import de.hpi.tfm.data.tfmp_input.table.TemporalDatabaseTableTrait

@SerialVersionUID(3L)
class AssociationMatch[A](val firstMatchPartner:TemporalDatabaseTableTrait[A],
                          val secondMatchPartner:TemporalDatabaseTableTrait[A],
                          val evidence:Int,
                          val changeBenefit:(Int,Int),
                          val isHeuristic:Boolean,
                          val tupleMapping:Option[TupleSetMatching[A]]) extends Serializable{

  override def toString: String = s"(${firstMatchPartner},$secondMatchPartner){$evidence} --> [${changeBenefit._1},${changeBenefit._2}])"

}
