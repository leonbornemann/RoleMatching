package de.hpi.tfm.compatibility.graph.fact

@SerialVersionUID(3L)
case class General_Many_To_Many_TupleMatching[A](tupleReferences: Seq[TupleReference[A]], evidence: Int, changeRange: (Int, Int)) extends Serializable {

  //TODO:calculate score if necessary
}
