package de.hpi.role_matching.playground

case class TableVersion(revisionID:BigInt,
                        revisionDate:String,
                        contributor:Contributor,
                        artificialColumnHeaders:IndexedSeq[String],
                        cells:IndexedSeq[IndexedSeq[TableCell]],
                        globalNaturalKeys:IndexedSeq[String],
                        localNaturalKeys:IndexedSeq[String],
                       ) {

}
